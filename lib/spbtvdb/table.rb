require "date"
require 'spbtvdb/source'

module Spbtvdb
  class Table
    BYTES_PER_NUMBER = 8
    BYTES_PER_DATE = 8

    def initialize(table_name)
      @table_name = table_name
      @columns = []
      @mask = ""
      @record_size = 0
    end

    def source(source_type, source_path)
      @source = case source_type
        when :file then FileSource.new(source_path)
      end
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
      column = case column_type
        when :integer then create_integer_column(column_name)
        when :datetime then create_datetime_column(column_name)
        else create_string_column(column_name, char_count)
      end
      @record_size += column[:size]
    end

    def slice(hash, keys)
      hash.select { |k, v| keys.include?(k) }
    end

    def select(query)
      result = [] 
      read(query[:mode] || :all_at_once) do |records|
        records = [records] unless records.is_a?(Array)
        records = records.select { |record| slice(record, query[:where].keys) == query[:where] } if query[:where]
        result += records
      end
      result = result.first(query[:limit]) if query[:limit]
      puts result.inspect
      result
    end  

    def read(mode, &block)
      offset = 0
      records = [] if mode == :all_at_once
      @source.open
      while data = @source.read(@record_size, @mask) do 
        record = { :offset => offset }
        @columns.each_with_index do |column, index|
          record[column[:name]] = case column[:type] 
            when :datetime then Time.at(data[index].to_i).to_date
            when :integer then data[index].to_i
            else data[index].to_s.strip
          end
        end
        if mode == :all_at_once
          records << record
        else
          block.call(record) 
        end
        offset += 1
      end
      block.call(records) if mode == :all_at_once
      @source.close
    end
  end
end