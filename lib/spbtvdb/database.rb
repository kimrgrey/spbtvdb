require "spbtvdb/table"

module Spbtvdb
 class Database
    def self.define_connection(connection_name, &block)
      db = Database.new(connection_name)
      @connections ||= {}
      @connections[connection_name] = db
      db.instance_eval(&block)
    end

    def initialize(connection_name) 
      @connection_name = connection_name
      @tables = {}
    end

    def table(table_name, &block)
      table = Table.new(table_name)
      table.instance_eval(&block)
      @tables[table_name] = table
    end

    def self.connect(connection_name)
      @connections[connection_name]
    end

    def select(table_name, query = {})
      table = @tables[table_name]
      table.select(query) if table
    end
  end
end