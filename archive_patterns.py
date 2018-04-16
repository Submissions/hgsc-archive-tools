"""The formats of the manual and automatic archives."""

import re

from wh_lib import Record


class ArchivePattern(Record):
    def __init__(self, pattern, offset, timestamp_group, archive, **kwds):
        super(ArchivePattern, self).__init__(pattern=pattern,
                                             offset=offset,
                                             timestamp_group=timestamp_group,
                                             archive=archive,
                                             **kwds)

    def match(self, path):
        """Return (archive_time_str, archive, original_path)
        or (None, None, None)."""
        m = self.pattern.match(path)
        if not m:
            return None, None, None
        timestamp = m.group(self.timestamp_group)
        archive_time_str = time_str_to_iso(timestamp)
        archive = self.archive
        original_path = path[self.offset:]
        if original_path.startswith('/snfs'):
            assert archive == 'manual'
            original_path = '/stornext' + original_path
        return archive_time_str, archive, original_path


ArchivePattern.AUTO_PATTERN = ArchivePattern(  # Automatic archive pattern
    pattern=re.compile(r'/stornext/snfsa/archive/archive_jobs/'
                       r'20(\d\d)/(\d\d)/(20\1\2\d{6})/'),
    offset=57,
    timestamp_group=3,
    archive = 'auto'
)


ArchivePattern.MANUAL_PATTERN = ArchivePattern(  # Manual archive pattern
    pattern=re.compile(r'/stornext/snfsa/archive/(20\d{10})/'),
    offset=36,
    timestamp_group=1,
    archive = 'manual'
)


def time_str_to_iso(timestamp):
    """input='201401021255'; output='2014-01-02T12:55:00'."""
    # 2014-01-02T12:55:00Z
    # 201401021255
    # 0123456789012
    #           1  
    assert len(timestamp) == 12
    yr = timestamp[:4]
    mt = timestamp[4:6]
    dy = timestamp[6:8]
    hr = timestamp[8:10]
    mn = timestamp[10:12]
    return '%s-%s-%sT%s:%s:00' % (yr, mt, dy, hr, mn)
