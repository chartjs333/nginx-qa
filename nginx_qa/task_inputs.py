"""Bounded, inert intake of user task attachments. Never evaluates workbook code."""
from __future__ import annotations

import hashlib
import io
import re
import zipfile
from pathlib import PurePosixPath
from xml.etree import ElementTree as ET

MAX_ATTACHMENT = 2 * 1024 * 1024
NS = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}


def workbook_summary(raw: bytes) -> dict:
    if not raw or len(raw) > MAX_ATTACHMENT:
        raise ValueError('ATTACHMENT_SIZE_INVALID')
    try:
        archive = zipfile.ZipFile(io.BytesIO(raw))
        entries = archive.infolist()
        names = [x.filename for x in entries]
        if (len(entries) > 256 or len(names) != len(set(names))
                or sum(x.file_size for x in entries) > 12 * 1024 * 1024
                or any(x.flag_bits & 1 or x.file_size > 4 * 1024 * 1024 for x in entries)
                or any(PurePosixPath(n).is_absolute() or '..' in PurePosixPath(n).parts or '\\' in n for n in names)):
            raise ValueError('WORKBOOK_ARCHIVE_LIMIT')
        if any('vbaproject' in n.lower() or n.startswith('xl/externalLinks/') for n in names):
            raise ValueError('WORKBOOK_ACTIVE_CONTENT_FORBIDDEN')

        def xml(name):
            data = archive.read(name)
            if b'<!DOCTYPE' in data.upper() or b'<!ENTITY' in data.upper():
                raise ValueError('WORKBOOK_XML_FORBIDDEN')
            return ET.fromstring(data)

        strings = []
        if 'xl/sharedStrings.xml' in names:
            strings = [''.join(x.itertext()) for x in xml('xl/sharedStrings.xml').findall('s:si', NS)]
            if len(strings) > 20000:
                raise ValueError('WORKBOOK_CELL_LIMIT')
        workbook = xml('xl/workbook.xml')
        rels = {r.get('Id'): r.get('Target') for r in xml('xl/_rels/workbook.xml.rels')}
        sheets, total, characters = [], 0, 0
        for sheet in workbook.findall('s:sheets/s:sheet', NS):
            target = rels.get(sheet.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id'), '')
            target = target.lstrip('/') if target.startswith('/') else 'xl/' + target
            if '..' in PurePosixPath(target).parts or target not in names:
                raise ValueError('WORKBOOK_RELATIONSHIP_INVALID')
            rows = []
            for row in xml(target).findall('s:sheetData/s:row', NS):
                values = {}
                for cell in row.findall('s:c', NS):
                    value = cell.findtext('s:v', default='', namespaces=NS)
                    if cell.get('t') == 's':
                        value = strings[int(value)]
                    elif cell.get('t') == 'inlineStr':
                        value = ''.join(cell.find('s:is', NS).itertext())
                    formula = cell.findtext('s:f', default='', namespaces=NS)
                    if formula:
                        value = '[formula, not evaluated] ' + formula
                    if value:
                        reference = cell.get('r', '')
                        if not re.fullmatch(r'[A-Z]{1,3}[1-9][0-9]{0,6}', reference):
                            raise ValueError('WORKBOOK_CELL_INVALID')
                        values[reference] = value
                        total += 1
                        characters += len(value)
                if values:
                    rows.append(values)
                if total > 12000 or characters > 250000 or len(rows) > 3000:
                    raise ValueError('WORKBOOK_CELL_LIMIT')
            sheets.append({'name': sheet.get('name', ''), 'rows': rows})
        comments = []
        for name in names:
            if re.fullmatch(r'xl/(?:comments|comments/comment)[0-9]+\.xml', name):
                for c in xml(name).findall('s:commentList/s:comment', NS):
                    text = ''.join(c.find('s:text', NS).itertext())
                    comments.append({'cell': c.get('ref'), 'text': text[:4000]})
        return {'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw),
                'sheet_count': len(sheets), 'nonempty_cells': total,
                'sheets': sheets, 'comments': comments, 'content_role': 'untrusted_source_data'}
    except (zipfile.BadZipFile, KeyError, IndexError, TypeError, ET.ParseError, OverflowError) as exc:
        raise ValueError('WORKBOOK_INVALID') from exc
