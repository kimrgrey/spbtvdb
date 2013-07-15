# Spbtvdb

Simple gem that was created as test job for SPB TV. 

## Usage

First of all clone this gem source code:
```
  git clone git://github.com/kimrgrey/spbtvdb.git
  cd spbtv
```
Then build and install it:
```
  gem build spbtvdb.gemspec
  gem install spbtvdb-0.0.1.gem  
```
Ok, now you can try this gem using something like that:

```ruby
require 'rubygems'
require 'spbtvdb'
require 'date'

File.open("/tmp/days", "wb") do |f|
  [*1...300].each do |n|
    3.times do
      f.write(["Day #{n}", n, Date.ordinal(2012, n).to_time.to_i].pack("a50qq"))
    end
  end
end

def assert_equal(expected, given)
  raise "Assertion failed" unless expected == given
  puts "w00t!"
end

Spbtvdb::Database.define_connection :sample do
  table :days do
    # It's a binary file without any separators,
    # each record is a fixed-size bytes sequence.
    # (do not care about file validity)
    source :file, "/tmp/days"

    # column name, type and size in bytes
    # (do not care about multibyte charsets, 1 char = 1 byte)
    column :name, :string, 50

    # some types has default size
    column :number, :integer
    column :date, :datetime
  end

  table :weeks do
    source :file, "/tmp/weeks"
    column :name, :string, 30
    column :number, :integer
  end
end

connection = Spbtvdb::Database.connect(:sample)
result = connection.select(:days, where: { name: "Day 117" }, limit: 1)

# `offset` is a record position in a source file
assert_equal [{ name: "Day 117", number: 117, date: Date.ordinal(2012, 117), offset: 348 }], result
```
