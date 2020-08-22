
import json


from apache_beam.io.filebasedsink import FileBasedSink
from apache_beam.io.iobase import Write
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.transforms import PTransform



class _NewlineJsonSink(FileBasedSink):

    def __init__(
        self,
        file_path_prefix, 
        coder = StrUtf8Coder(), 
        file_name_suffix='.json', 
        num_shards=0, 
        shard_name_template=None, 
        mime_type='application/octet-stream', 
        compression_type='auto'
    ):
        super().__init__(
            file_path_prefix = file_path_prefix,
            file_name_suffix=file_name_suffix,
            num_shards=num_shards,
            shard_name_template=shard_name_template,
            coder=coder,
            mime_type='text/plain',
            compression_type=compression_type
        )

    def open(self, temp_path):
        file_handle = open(temp_path)
        return file_handle


    def write_record(self, file_handle, value):
        """Writes a single encoded record converted to JSON, preceded by a newline"""
        file_handle.write('\n')
        file_handle.write(self.coder.encode(json.dumps(value)))


    def close(self, file_handle):
        file_handle.close()


class WriteToNewlineJsonSink(PTransform):

    def __init__(
        self,
        file_path_prefix, 
        coder = StrUtf8Coder(), 
        file_name_suffix='.json', 
        num_shards=0, 
        shard_name_template=None, 
        mime_type='application/octet-stream', 
        compression_type='auto'
    ):
        super().__init__()

        self._sink = _NewlineJsonSink(
            file_path_prefix = file_path_prefix,
            coder = coder,
            file_name_suffix = file_name_suffix,
            num_shards = num_shards,
            shard_name_template = shard_name_template,
            mime_type = mime_type,
            compression_type = compression_type
        )


    def expand(self, pcoll):
        return pcoll | Write(self._sink)


