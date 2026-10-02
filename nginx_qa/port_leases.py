"""Process-local socket reservations for managed runtime port leases.

The durable lease rows live in the managed runtime database.  This module is
deliberately limited to the process-local side of that protocol: it keeps a
socket bound while an assignment is PREPARED and fences the short handoff to a
managed child.  It never writes durable state.

``ManagedPortReservationRegistry`` is safe to share by importer and supervisor
threads.  A registry owns an endpoint globally (across all database paths and
network-namespace labels) until the lease is explicitly released.  The
network-namespace label is durable identity, not proof that this Python process
can bind in a different operating-system namespace.
"""

from __future__ import annotations

from dataclasses import dataclass
import ipaddress
import os
from pathlib import Path
import socket
import threading
from typing import Any, Mapping, Sequence
from uuid import uuid4

from .sprint_types import canonical_json_sha256


_RESERVED = "reserved"
_HANDING_OFF = "handing_off"
_BOUND = "bound"
_VALID_RESERVATION_STATES = frozenset({_RESERVED, _HANDING_OFF, _BOUND})


class ManagedPortReservationError(RuntimeError):
    """Raised when a planned managed port cannot be safely reserved."""


@dataclass(frozen=True, slots=True)
class ManagedPortReservationToken:
    """Identify sockets newly acquired by one activation transaction.

    The shape intentionally matches the original managed-import token.  An
    idempotent acquisition therefore returns a token with no ``acquired_keys``;
    rolling it back leaves reservations acquired by an earlier transaction in
    place.
    """

    database_key: str
    acquired_keys: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class ManagedPortReconciliationFence:
    """Registry generation captured before reading a durable lease snapshot."""

    database_key: str
    generation: int


@dataclass(frozen=True, slots=True)
class ManagedPortHandoffToken:
    """A single-use capability for one reserved-socket handoff attempt."""

    database_key: str
    lease_id: str
    network_namespace_id: str
    host: str
    port: int
    signature: str
    generation: int
    nonce: str


@dataclass(frozen=True, slots=True)
class ManagedPortReservationSnapshot:
    """Immutable, socket-free view of a process-local reservation."""

    database_path: Path
    database_key: str
    lease_id: str
    network_namespace_id: str
    host: str
    port: int
    signature: str
    state: str
    generation: int
    holds_socket: bool


@dataclass(slots=True)
class _ManagedPortReservation:
    database_path: Path
    database_key: str
    lease_id: str
    network_namespace_id: str
    host: str
    port: int
    signature: str
    holder: socket.socket | None
    state: str = _RESERVED
    generation: int = 1
    handoff_nonce: str | None = None
    durable_seen: bool = False
    durable_revision: int = 0


@dataclass(frozen=True, slots=True)
class _RequestedLease:
    lease_id: str
    network_namespace_id: str
    host: str
    port: int
    signature: str
    endpoint_key: tuple[str, int]


class ManagedPortReservationRegistry:
    """Hold and hand off managed child ports across request-scoped objects.

    The registry provides two distinct ownership signals:

    * ``holds_endpoint`` means this process currently has a bound reservation
      socket.
    * ``owns_endpoint`` also includes a fenced handoff or a child-bound lease.

    A handoff closes the reservation socket because ordinary child programs
    bind their own listener.  It cannot prevent an unrelated OS process from
    winning that external bind race.  It does prevent a second lease in this
    registry from entering the race, and its capability token prevents a stale
    supervisor action from completing or undoing a newer handoff.  Callers must
    verify the child's exact process and socket ownership before calling
    ``complete_handoff``.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._durable_revision = 0
        self._reservations: dict[
            tuple[str, str], _ManagedPortReservation
        ] = {}
        self._endpoint_owners: dict[
            tuple[str, int], tuple[str, str]
        ] = {}

    def _next_durable_revision_locked(self) -> int:
        self._durable_revision += 1
        return self._durable_revision

    def _mark_durable_locked(self, reservation: _ManagedPortReservation) -> None:
        if reservation.durable_seen:
            return
        reservation.durable_seen = True
        reservation.durable_revision = self._next_durable_revision_locked()

    @staticmethod
    def _database_identity(
        database_path: str | os.PathLike[str],
    ) -> tuple[Path, str]:
        path = Path(database_path).resolve(strict=False)
        return path, os.path.normcase(str(path))

    @staticmethod
    def _endpoint_identity(host: str, port: int) -> tuple[str, int]:
        try:
            normalized_host = ipaddress.ip_address(host).compressed.casefold()
        except ValueError:
            normalized_host = host.casefold()
        return normalized_host, port

    @staticmethod
    def _lease_signature(raw: Mapping[str, Any]) -> str:
        """Hash immutable identity while normalizing lifecycle-only fields."""

        normalized = dict(raw)
        normalized.update(
            {
                "process_id": None,
                "status": _RESERVED,
                "bind_verified": False,
                "released_at": None,
            }
        )
        return canonical_json_sha256(normalized)

    @staticmethod
    def _bind(host: str, port: int) -> socket.socket:
        family = socket.AF_INET6 if ":" in host else socket.AF_INET
        candidate = socket.socket(family, socket.SOCK_STREAM)
        try:
            candidate.set_inheritable(False)
            if family == socket.AF_INET6 and hasattr(socket, "IPV6_V6ONLY"):
                candidate.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            if os.name == "nt" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                candidate.setsockopt(
                    socket.SOL_SOCKET,
                    socket.SO_EXCLUSIVEADDRUSE,
                    1,
                )
            candidate.bind((host, port))
        except BaseException:
            candidate.close()
            raise
        return candidate

    @classmethod
    def _validated_plan(
        cls,
        leases: Sequence[Mapping[str, Any]],
    ) -> tuple[_RequestedLease, ...]:
        requested: list[_RequestedLease] = []
        seen_leases: set[str] = set()
        seen_endpoints: set[tuple[str, int]] = set()
        try:
            for raw in leases:
                if not isinstance(raw, Mapping):
                    raise ValueError
                lease_id = raw.get("lease_id")
                namespace = raw.get("network_namespace_id")
                host = raw.get("host")
                port = raw.get("port")
                if (
                    not isinstance(lease_id, str)
                    or not lease_id
                    or not isinstance(namespace, str)
                    or not namespace
                    or not isinstance(host, str)
                    or not host
                    or not isinstance(port, int)
                    or isinstance(port, bool)
                    or not (1 <= port <= 65535)
                ):
                    raise ValueError
                endpoint_key = cls._endpoint_identity(host, port)
                if lease_id in seen_leases or endpoint_key in seen_endpoints:
                    raise ValueError
                seen_leases.add(lease_id)
                seen_endpoints.add(endpoint_key)
                requested.append(
                    _RequestedLease(
                        lease_id=lease_id,
                        network_namespace_id=namespace,
                        host=host,
                        port=port,
                        signature=cls._lease_signature(raw),
                        endpoint_key=endpoint_key,
                    )
                )
        except (TypeError, ValueError, OverflowError) as exc:
            raise ManagedPortReservationError(
                "invalid managed port plan"
            ) from exc
        return tuple(sorted(requested, key=lambda item: item.lease_id))

    @staticmethod
    def _matches(
        reservation: _ManagedPortReservation,
        requested: _RequestedLease,
    ) -> bool:
        return (
            reservation.network_namespace_id == requested.network_namespace_id
            and reservation.host == requested.host
            and reservation.port == requested.port
            and reservation.signature == requested.signature
        )

    @staticmethod
    def _holder_is_open(reservation: _ManagedPortReservation) -> bool:
        holder = reservation.holder
        if holder is None or holder.fileno() < 0:
            return False
        try:
            bound = holder.getsockname()
        except OSError:
            return False
        return bool(bound) and int(bound[1]) == reservation.port

    @staticmethod
    def _snapshot_of(
        reservation: _ManagedPortReservation,
    ) -> ManagedPortReservationSnapshot:
        return ManagedPortReservationSnapshot(
            database_path=reservation.database_path,
            database_key=reservation.database_key,
            lease_id=reservation.lease_id,
            network_namespace_id=reservation.network_namespace_id,
            host=reservation.host,
            port=reservation.port,
            signature=reservation.signature,
            state=reservation.state,
            generation=reservation.generation,
            holds_socket=ManagedPortReservationRegistry._holder_is_open(
                reservation
            ),
        )

    def _release_keys_locked(self, keys: Sequence[tuple[str, str]]) -> None:
        for key in keys:
            reservation = self._reservations.pop(key, None)
            if reservation is None:
                continue
            endpoint_key = self._endpoint_identity(
                reservation.host, reservation.port
            )
            if self._endpoint_owners.get(endpoint_key) == key:
                self._endpoint_owners.pop(endpoint_key, None)
            holder = reservation.holder
            reservation.holder = None
            if holder is not None:
                holder.close()

    def _prune_locked(self) -> None:
        stale = [
            key
            for key, reservation in self._reservations.items()
            if not reservation.database_path.exists()
        ]
        self._release_keys_locked(stale)

    def _acquire_validated_locked(
        self,
        path: Path,
        database_key: str,
        requested: Sequence[_RequestedLease],
        *,
        durable: bool = False,
    ) -> ManagedPortReservationToken:
        acquired: list[tuple[str, str]] = []
        try:
            for lease in requested:
                key = (database_key, lease.lease_id)
                existing = self._reservations.get(key)
                if existing is not None:
                    if not self._matches(existing, lease):
                        raise ManagedPortReservationError(
                            "managed port lease identity changed"
                        )
                    if durable:
                        self._mark_durable_locked(existing)
                    continue
                endpoint_owner = self._endpoint_owners.get(lease.endpoint_key)
                if endpoint_owner is not None and endpoint_owner != key:
                    raise ManagedPortReservationError(
                        "managed child port is already owned"
                    )
                holder = self._bind(lease.host, lease.port)
                self._reservations[key] = _ManagedPortReservation(
                    database_path=path,
                    database_key=database_key,
                    lease_id=lease.lease_id,
                    network_namespace_id=lease.network_namespace_id,
                    host=lease.host,
                    port=lease.port,
                    signature=lease.signature,
                    holder=holder,
                    durable_seen=durable,
                    durable_revision=self._next_durable_revision_locked(),
                )
                self._endpoint_owners[lease.endpoint_key] = key
                acquired.append(key)
        except (OSError, ManagedPortReservationError) as exc:
            self._release_keys_locked(acquired)
            raise ManagedPortReservationError(
                "managed child port is already owned"
            ) from exc
        return ManagedPortReservationToken(database_key, tuple(acquired))

    def _handoff_reservation_locked(
        self,
        token: ManagedPortHandoffToken,
    ) -> _ManagedPortReservation:
        key = (token.database_key, token.lease_id)
        reservation = self._reservations.get(key)
        if (
            reservation is None
            or reservation.state != _HANDING_OFF
            or reservation.handoff_nonce != token.nonce
            or reservation.generation != token.generation
            or reservation.network_namespace_id != token.network_namespace_id
            or reservation.host != token.host
            or reservation.port != token.port
            or reservation.signature != token.signature
        ):
            raise ManagedPortReservationError(
                "managed port handoff token is stale"
            )
        return reservation

    def prune(self) -> None:
        """Release holders whose durable runtime database was removed."""

        with self._lock:
            self._prune_locked()

    def reconcile_durable(
        self,
        database_path: str | os.PathLike[str],
        live_leases: Sequence[Mapping[str, Any]],
        *,
        fence: ManagedPortReconciliationFence,
    ) -> None:
        """Forget local claims no longer live in the durable database.

        Another service instance can commit a terminal process transition, but
        it cannot directly close this process's reservation socket or clear its
        in-memory bound claim.  Callers provide the authoritative set of
        ``reserved``/``bound`` leases read from the shared database.  Exact
        live identities are retained; released or removed identities are
        closed locally before this registry plans or launches more work.
        """

        _, database_key = self._database_identity(database_path)
        if fence.database_key != database_key:
            raise ManagedPortReservationError(
                "managed port reconciliation fence belongs to another database"
            )
        requested = self._validated_plan(live_leases)
        requested_by_id = {lease.lease_id: lease for lease in requested}
        with self._lock:
            self._prune_locked()
            if fence.generation > self._durable_revision:
                raise ManagedPortReservationError(
                    "managed port reconciliation fence is from the future"
                )
            stale: list[tuple[str, str]] = []
            for key, reservation in self._reservations.items():
                if key[0] != database_key:
                    continue
                durable = requested_by_id.get(key[1])
                if durable is None:
                    if (
                        reservation.durable_seen
                        and reservation.durable_revision <= fence.generation
                    ):
                        stale.append(key)
                    continue
                if not self._matches(reservation, durable):
                    if reservation.durable_revision > fence.generation:
                        # This local identity was acquired after the caller
                        # began reading its SQLite snapshot.  A later snapshot
                        # is authoritative; this one cannot invalidate it.
                        continue
                    raise ManagedPortReservationError(
                        "durable managed port lease identity changed"
                    )
                self._mark_durable_locked(reservation)
            self._release_keys_locked(stale)

    def reconciliation_fence(
        self,
        database_path: str | os.PathLike[str],
    ) -> ManagedPortReconciliationFence:
        """Fence reservations created after a durable snapshot starts."""

        _, database_key = self._database_identity(database_path)
        with self._lock:
            self._prune_locked()
            return ManagedPortReconciliationFence(
                database_key=database_key,
                generation=self._durable_revision,
            )

    def mark_durable_many(
        self,
        database_path: str | os.PathLike[str],
        leases: Sequence[Mapping[str, Any]],
    ) -> None:
        """Finalize exact reservations immediately after their SQLite commit."""

        _, database_key = self._database_identity(database_path)
        requested = self._validated_plan(leases)
        with self._lock:
            self._prune_locked()
            reservations: list[_ManagedPortReservation] = []
            for lease in requested:
                reservation = self._reservations.get((database_key, lease.lease_id))
                if reservation is None or not self._matches(reservation, lease):
                    raise ManagedPortReservationError(
                        "committed managed port reservation is missing or changed"
                    )
                reservations.append(reservation)
            for reservation in reservations:
                self._mark_durable_locked(reservation)

    def acquire_many(
        self,
        database_path: str | os.PathLike[str],
        leases: Sequence[Mapping[str, Any]],
    ) -> ManagedPortReservationToken:
        """Bind a whole port plan, rolling back this call's partial batch.

        Repeating an identical acquisition is idempotent.  An existing lease ID
        with any changed durable field is rejected because the signature covers
        the complete lease mapping.
        """

        path, database_key = self._database_identity(database_path)
        requested = self._validated_plan(leases)
        with self._lock:
            self._prune_locked()
            return self._acquire_validated_locked(
                path, database_key, requested
            )

    def acquire_durable_many(
        self,
        database_path: str | os.PathLike[str],
        leases: Sequence[Mapping[str, Any]],
    ) -> ManagedPortReservationToken:
        """Bind leases already committed in the shared runtime database."""

        path, database_key = self._database_identity(database_path)
        requested = self._validated_plan(leases)
        with self._lock:
            self._prune_locked()
            return self._acquire_validated_locked(
                path,
                database_key,
                requested,
                durable=True,
            )

    def recover_exact(
        self,
        database_path: str | os.PathLike[str],
        leases: Sequence[Mapping[str, Any]],
    ) -> ManagedPortReservationToken:
        """Reacquire an exact durable PREPARED lease set after restart.

        Unlike ``acquire_many``, this operation rejects an unlisted reservation
        already associated with the database.  Existing listed leases must
        still have their reservation sockets; a ``handing_off`` or ``bound``
        claim is never mistaken for restart recovery.  Missing listed leases
        are acquired atomically with respect to this registry.
        """

        path, database_key = self._database_identity(database_path)
        requested = self._validated_plan(leases)
        requested_ids = {lease.lease_id for lease in requested}
        with self._lock:
            self._prune_locked()
            current_ids = {
                lease_id
                for key_database, lease_id in self._reservations
                if key_database == database_key
            }
            if current_ids - requested_ids:
                raise ManagedPortReservationError(
                    "managed port recovery plan does not match registry"
                )
            for lease in requested:
                existing = self._reservations.get(
                    (database_key, lease.lease_id)
                )
                if existing is None:
                    continue
                if not self._matches(existing, lease):
                    raise ManagedPortReservationError(
                        "managed port recovery plan does not match registry"
                    )
                if (
                    existing.state != _RESERVED
                    or not self._holder_is_open(existing)
                ):
                    raise ManagedPortReservationError(
                        "managed port lease is not held for recovery"
                    )
            return self._acquire_validated_locked(
                path,
                database_key,
                requested,
                durable=True,
            )

    def reacquire_many(
        self,
        database_path: str | os.PathLike[str],
        leases: Sequence[Mapping[str, Any]],
    ) -> ManagedPortReservationToken:
        """Compatibility-friendly name for exact restart recovery."""

        return self.recover_exact(database_path, leases)

    def begin_handoff(
        self,
        database_path: str | os.PathLike[str],
        lease_id: str,
        *,
        expected_signature: str | None = None,
    ) -> ManagedPortHandoffToken:
        """Close one holder and return a capability fencing that handoff.

        The endpoint remains claimed inside this registry.  The caller should
        spawn the child, verify exact process-to-socket ownership, and then call
        ``complete_handoff``.  On launch failure, ``reacquire_handoff`` attempts
        to restore the reservation without weakening the internal claim.
        """

        _, database_key = self._database_identity(database_path)
        with self._lock:
            self._prune_locked()
            reservation = self._reservations.get((database_key, lease_id))
            if reservation is None:
                raise ManagedPortReservationError(
                    "managed port lease is not reserved"
                )
            if (
                expected_signature is not None
                and reservation.signature != expected_signature
            ):
                raise ManagedPortReservationError(
                    "managed port lease identity changed"
                )
            if (
                reservation.state != _RESERVED
                or not self._holder_is_open(reservation)
            ):
                raise ManagedPortReservationError(
                    "managed port lease is not reserved"
                )
            holder = reservation.holder
            reservation.holder = None
            reservation.state = _HANDING_OFF
            reservation.generation += 1
            reservation.handoff_nonce = uuid4().hex
            assert holder is not None
            holder.close()
            return ManagedPortHandoffToken(
                database_key=reservation.database_key,
                lease_id=reservation.lease_id,
                network_namespace_id=reservation.network_namespace_id,
                host=reservation.host,
                port=reservation.port,
                signature=reservation.signature,
                generation=reservation.generation,
                nonce=reservation.handoff_nonce,
            )

    def complete_handoff(
        self,
        token: ManagedPortHandoffToken,
    ) -> ManagedPortReservationSnapshot:
        """Mark a verified child bind complete using its exact capability."""

        with self._lock:
            self._prune_locked()
            reservation = self._handoff_reservation_locked(token)
            reservation.state = _BOUND
            reservation.handoff_nonce = None
            reservation.generation += 1
            return self._snapshot_of(reservation)

    def reacquire_handoff(
        self,
        token: ManagedPortHandoffToken,
    ) -> ManagedPortReservationSnapshot:
        """Undo a failed handoff by rebinding the exact reserved endpoint.

        If another process has already bound the endpoint, the fenced handoff
        claim is retained and this method raises.  The caller can then inspect
        process ownership and either retry or complete the same handoff.
        """

        with self._lock:
            self._prune_locked()
            reservation = self._handoff_reservation_locked(token)
            try:
                holder = self._bind(reservation.host, reservation.port)
            except OSError as exc:
                raise ManagedPortReservationError(
                    "managed child port is already owned"
                ) from exc
            reservation.holder = holder
            reservation.state = _RESERVED
            reservation.handoff_nonce = None
            reservation.generation += 1
            return self._snapshot_of(reservation)

    def abort_handoff(
        self,
        token: ManagedPortHandoffToken,
    ) -> ManagedPortReservationSnapshot:
        """Alias for ``reacquire_handoff`` used by launch-failure paths."""

        return self.reacquire_handoff(token)

    def reacquire_bound(
        self,
        database_path: str | os.PathLike[str],
        lease_id: str,
        *,
        expected_signature: str,
        expected_generation: int | None = None,
    ) -> ManagedPortReservationSnapshot:
        """Rebind an exact child-bound claim after verified process death.

        Process death and PID identity checks are supervisor responsibilities.
        This method only accepts the exact signature (and optionally generation)
        observed by that supervisor.  A still-live listener makes the bind fail
        and leaves the claim unchanged.
        """

        _, database_key = self._database_identity(database_path)
        with self._lock:
            self._prune_locked()
            reservation = self._reservations.get((database_key, lease_id))
            if (
                reservation is None
                or reservation.state != _BOUND
                or reservation.signature != expected_signature
                or (
                    expected_generation is not None
                    and reservation.generation != expected_generation
                )
            ):
                raise ManagedPortReservationError(
                    "managed port bound claim is stale"
                )
            try:
                holder = self._bind(reservation.host, reservation.port)
            except OSError as exc:
                raise ManagedPortReservationError(
                    "managed child port is already owned"
                ) from exc
            reservation.holder = holder
            reservation.state = _RESERVED
            reservation.generation += 1
            return self._snapshot_of(reservation)

    def reservation(
        self,
        database_path: str | os.PathLike[str],
        lease_id: str,
    ) -> ManagedPortReservationSnapshot | None:
        """Return an immutable view of one lease, if this registry owns it."""

        _, database_key = self._database_identity(database_path)
        with self._lock:
            self._prune_locked()
            reservation = self._reservations.get((database_key, lease_id))
            return (
                None
                if reservation is None
                else self._snapshot_of(reservation)
            )

    def reservations(
        self,
        database_path: str | os.PathLike[str] | None = None,
    ) -> tuple[ManagedPortReservationSnapshot, ...]:
        """Return stable snapshots sorted by database and lease identity."""

        database_key: str | None = None
        if database_path is not None:
            _, database_key = self._database_identity(database_path)
        with self._lock:
            self._prune_locked()
            return tuple(
                self._snapshot_of(reservation)
                for key, reservation in sorted(self._reservations.items())
                if database_key is None or key[0] == database_key
            )

    def endpoint_owner(
        self,
        host: str,
        port: int,
    ) -> ManagedPortReservationSnapshot | None:
        """Return the registry owner of a normalized host/port endpoint."""

        if (
            not isinstance(host, str)
            or not host
            or not isinstance(port, int)
            or isinstance(port, bool)
            or not (1 <= port <= 65535)
        ):
            return None
        endpoint_key = self._endpoint_identity(host, port)
        with self._lock:
            self._prune_locked()
            owner_key = self._endpoint_owners.get(endpoint_key)
            if owner_key is None:
                return None
            reservation = self._reservations.get(owner_key)
            return (
                None
                if reservation is None
                else self._snapshot_of(reservation)
            )

    def owns_endpoint(
        self,
        database_path: str | os.PathLike[str],
        host: str,
        port: int,
    ) -> bool:
        """Return whether the database owns this endpoint in any live phase."""

        _, database_key = self._database_identity(database_path)
        owner = self.endpoint_owner(host, port)
        return owner is not None and owner.database_key == database_key

    def holds_endpoint(
        self,
        database_path: str | os.PathLike[str],
        host: str,
        port: int,
    ) -> bool:
        """Return whether this process currently holds the reservation socket."""

        _, database_key = self._database_identity(database_path)
        owner = self.endpoint_owner(host, port)
        return (
            owner is not None
            and owner.database_key == database_key
            and owner.state == _RESERVED
            and owner.holds_socket
        )

    def rollback(self, token: ManagedPortReservationToken) -> None:
        """Release sockets first acquired by an uncommitted transaction."""

        with self._lock:
            keys = tuple(
                key
                for key in token.acquired_keys
                if len(key) == 2 and key[0] == token.database_key
            )
            self._release_keys_locked(keys)

    def release(
        self,
        database_path: str | os.PathLike[str],
        lease_id: str,
    ) -> None:
        """Release one claim for supervisor handoff or terminal cleanup."""

        _, database_key = self._database_identity(database_path)
        with self._lock:
            self._release_keys_locked(((database_key, lease_id),))

    def release_exact(
        self,
        database_path: str | os.PathLike[str],
        lease_id: str,
        *,
        expected_signature: str,
        expected_generation: int | None = None,
        expected_state: str | None = None,
    ) -> bool:
        """Release a claim only if the caller's immutable evidence still matches."""

        if (
            expected_state is not None
            and expected_state not in _VALID_RESERVATION_STATES
        ):
            raise ManagedPortReservationError(
                "invalid managed port reservation state"
            )
        _, database_key = self._database_identity(database_path)
        key = (database_key, lease_id)
        with self._lock:
            self._prune_locked()
            reservation = self._reservations.get(key)
            if reservation is None:
                return False
            if (
                reservation.signature != expected_signature
                or (
                    expected_generation is not None
                    and reservation.generation != expected_generation
                )
                or (
                    expected_state is not None
                    and reservation.state != expected_state
                )
            ):
                raise ManagedPortReservationError(
                    "managed port release evidence is stale"
                )
            self._release_keys_locked((key,))
            return True

    def close_all(self) -> None:
        """Close every holder and forget every process-local claim."""

        with self._lock:
            self._release_keys_locked(tuple(self._reservations))


__all__ = [
    "ManagedPortHandoffToken",
    "ManagedPortReservationError",
    "ManagedPortReservationRegistry",
    "ManagedPortReservationSnapshot",
    "ManagedPortReservationToken",
]
