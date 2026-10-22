"""
Tests for stream reader/writer.
"""

import pytest
from eventloop import (
    run,
    StreamReader,
    StreamWriter,
    LimitOverrunError,
    IncompleteReadError,
)


class TestStreamReader:
    """Test StreamReader."""
    
    def test_read_basic(self):
        async def test():
            reader = StreamReader()
            reader.feed_data(b"hello")
            reader.feed_eof()
            
            data = await reader.read(5)
            assert data == b"hello"
        
        run(test())
    
    def test_read_all(self):
        async def test():
            reader = StreamReader()
            reader.feed_data(b"hello")
            reader.feed_data(b" world")
            reader.feed_eof()
            
            data = await reader.read()
            assert data == b"hello world"
        
        run(test())
    
    def test_readexactly(self):
        async def test():
            reader = StreamReader()
            reader.feed_data(b"hello")
            reader.feed_eof()
            
            data = await reader.readexactly(5)
            assert data == b"hello"
        
        run(test())
    
    def test_readexactly_incomplete(self):
        async def test():
            reader = StreamReader()
            reader.feed_data(b"he")
            reader.feed_eof()
            
            with pytest.raises(IncompleteReadError) as exc_info:
                await reader.readexactly(5)
            
            assert exc_info.value.partial == b"he"
            assert exc_info.value.expected == 5
        
        run(test())
    
    def test_readuntil(self):
        async def test():
            reader = StreamReader()
            reader.feed_data(b"line1\nline2\n")
            reader.feed_eof()
            
            line1 = await reader.readuntil(b"\n")
            assert line1 == b"line1\n"
            
            line2 = await reader.readuntil(b"\n")
            assert line2 == b"line2\n"
        
        run(test())
    
    def test_readline(self):
        async def test():
            reader = StreamReader()
            reader.feed_data(b"hello\nworld\n")
            reader.feed_eof()
            
            line1 = await reader.readline()
            assert line1 == b"hello\n"
            
            line2 = await reader.readline()
            assert line2 == b"world\n"
        
        run(test())
    
    def test_limit_overrun(self):
        async def test():
            reader = StreamReader(limit=10)
            reader.feed_data(b"12345678901")  # 11 bytes, limit is 10
            
            # Should raise when trying to read
            with pytest.raises(LimitOverrunError):
                await reader.read()
        
        run(test())
    
    def test_iter_lines(self):
        async def test():
            reader = StreamReader()
            reader.feed_data(b"line1\nline2\nline3\n")
            reader.feed_eof()
            
            lines = []
            async for line in reader:
                lines.append(line)
            
            assert lines == [b"line1\n", b"line2\n", b"line3\n"]
        
        run(test())
    
    def test_at_eof(self):
        async def test():
            reader = StreamReader()
            assert not reader.at_eof
            
            reader.feed_data(b"data")
            assert not reader.at_eof
            
            await reader.read(4)
            assert not reader.at_eof  # Not EOF until feed_eof
            
            reader.feed_eof()
            assert reader.at_eof
        
        run(test())


class TestStreamWriter:
    """Test StreamWriter."""
    
    def test_write_basic(self):
        async def test():
            # Mock transport
            class MockTransport:
                def __init__(self):
                    self.data = b""
                
                def write(self, data):
                    self.data += data
                
                def close(self):
                    pass
            
            transport = MockTransport()
            writer = StreamWriter(transport, None)
            
            writer.write(b"hello")
            writer.write(b" world")
            
            assert transport.data == b"hello world"
        
        run(test())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])