require "date"

module Spbtvdb
  class Table
    BYTES_PER_NUMBER = 8
    BYTES_PER_DATE = 8

    def initialize(table_name)
      @columns = []
      @mask = ""
      @record_size = 0
      @table_name = table_name
    end

    def source (source_type, source_path)
      @source_type = source_type
      @source_path = source_path
    end

    def create_integer_column(column_name)
      column = {:name => column_name, :type => :integer, :size => BYTES_PER_NUMBER}
      @columns << column
      @mask << "q"
      column
    end

    def create_datetime_column(column_name)
      column = {:name => column_name, :type => :datetime, :size => BYTES_PER_DATE}
      @columns << column
      @mask << "q"
      column
    end

    def create_string_column(column_name, char_count)
      column = {:name => column_name, :type => :string, :size => char_count}
      @columns << column
      @mask << "a#{char_count}"
      column
    end

    def column(column_name, column_type, char_count = nil)
      # FIXME There is a nice way to handle types?
      column = case column_type
        when :integer then create_integer_column(column_name)
        when :datetime then create_datetime_column(column_name)
        else create_string_column(column_name, char_count)
      end
      # FIXME And what about bad data type?
      @record_size += column[:size]
    end

    def slice(hash, keys)
      hash.select { |k, v| keys.include?(k) }
    end

    def select(query)
      read_from_source unless ready_to_use?
      result = @records
      result = result.select { |record| slice(record, query[:where].keys) == query[:where] } if query[:where]
      result = result.first(query[:limit]) if query[:limit]
      result
    end

    def ready_to_use?
      @records
    end

    def read_from_source
      @records = []
      File.open(@source_path, "rb") do |f|
        offset = 0
        while buf = f.read(@record_size) do 
          data = buf.unpack(@mask)
          record = { :offset => offset }
          @columns.each_with_index do |column, index|
            record[column[:name]] = case column[:type] 
              when :datetime then Time.at(data[index].to_i).to_date
              when :integer then data[index].to_i
              else data[index].to_s.strip
            end
          end
          @records << record
          offset += 1
        end
      end
    end
  end
end