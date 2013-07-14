module Spbtvdb
  class Source
    def initialize(path)
      @path = path
    end

    def open
    end

    def close
    end

    def read(record_size, mask)
      raise NotImplementedError
    end
  end

  class FileSource < Source
    def open
      @file = File.open(@path, "rb")
    end

    def close
      @file.close if @file
      @file = nil
    end

    def read(record_size, mask)
      buf = @file.read(record_size)
      buf.unpack(mask) if buf  
    end
  end
end