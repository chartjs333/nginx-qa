"""Exercise the private deployment override without starting any host."""
import os
import unittest

from tests.test_staging_isolation import run_powershell, assert_powershell_success


@unittest.skipUnless(os.name == "nt", "PowerShell behavior is Windows-specific")
class DeploymentBranchTests(unittest.TestCase):
    def test_defaults_and_explicit_wizard_identities(self):
        result = run_powershell('''
            foreach ($profile in @("umse-007", "telegram-managed-sprint-launcher-v1", "setup-diagnostics-wizard-v1")) {
                $actual = Resolve-StagingDeploymentBranch -Profile $profile -DefaultBranch "retained-default"
                if ($actual -cne "retained-default") { throw "Default changed" }
            }
            foreach ($branch in @("codex/sdw-qualified-host-20261104", "codex/sdw-qualified-host-continuation-20261104", "codex/sdw-qualified-ui-recovery-20261104")) {
                $actual = Resolve-StagingDeploymentBranch -Profile "setup-diagnostics-wizard-v1" -DefaultBranch "old" -DeploymentBranch $branch
                if ($actual -cne $branch) { throw "Approved identity rejected" }
            }
        ''')
        assert_powershell_success(self, result)

    def test_other_profiles_and_unapproved_ref_forms_are_rejected(self):
        result = run_powershell('''
            $cases = @(
                @("umse-007", "codex/sdw-qualified-host-continuation-20261104"),
                @("umse-007", "codex/sdw-qualified-ui-recovery-20261104"),
                @("setup-diagnostics-wizard-v1", "codex/sdw-qualified-ui-recovery-20261104; exit"),
                @("telegram-managed-sprint-launcher-v1-gateway", "codex/sdw-qualified-host-20261104"),
                @("setup-diagnostics-wizard-v1", "main"),
                @("setup-diagnostics-wizard-v1", "refs/heads/codex/sdw-qualified-host-continuation-20261104"),
                @("setup-diagnostics-wizard-v1", "CODEX/sdw-qualified-host-continuation-20261104"),
                @("setup-diagnostics-wizard-v1", "codex/sdw-qualified-host-continuation-20261104 "),
                @("setup-diagnostics-wizard-v1", " "),
                @("setup-diagnostics-wizard-v1", "codex/sdw-qualified-host-continuation-20261104; exit")
            )
            foreach ($case in $cases) {
                $rejected = $false
                try { $null = Resolve-StagingDeploymentBranch -Profile $case[0] -DefaultBranch "old" -DeploymentBranch $case[1] }
                catch { $rejected = $true }
                if (-not $rejected) { throw "Unapproved deployment identity accepted" }
            }
        ''')
        assert_powershell_success(self, result)
