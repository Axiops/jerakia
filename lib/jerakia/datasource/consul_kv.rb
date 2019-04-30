require 'diplomat'
require 'hcl/checker'
require 'uri'

class Jerakia::Datasource::Consul_kv < Jerakia::Datasource::Instance

  # datacenter option sets the :dc method in the Diplomat
  # request to perform a consul lookup using a particular
  # datacenter
  #
  option(:datacenter) { |str| str.is_a?(String) }

  # parse_values will try to parse the value under each key
  # Useful for inserting data under a single key
  # It will attempt to parse HCL, JSON and YAML in that order
  #
  option(:parse_values, :default => false) { |opt|
    [ TrueClass, FalseClass ].include?(opt.class)
  }

  # The searchpath is the root path of the request, which will
  # get appended with the namespace, and key if applicable.
  # We allow this option to be ommited in case the lookup path
  # starts at the namesapce.
  #
  option(:searchpath, :default => ['']) { |opt| opt.is_a?(Array) }

  # recursive will return the entire data structure from consul
  # rather than just the requested key
  #
  option(:recursive, :default => false) { |opt|
    [ TrueClass, FalseClass ].include?(opt.class)
  }

  # to_hash, when used with recursive, will consolidate the
  # results into a hash, instead of an array
  #
  option(:to_hash, :default => true) { |opt|
    [ TrueClass, FalseClass ].include?(opt.class)
  }

  # traverse_hash, when used with recursive and parse_values
  # will search for all the keys in the results hash and parse the value
  # for each one
  #
  option(:traverse_hash, :default => false) { |opt|
    [ TrueClass, FalseClass ].include?(opt.class)
  }

  # Set any consul parameters against the Diplomat class
  #
  # These values are set in jerakia.yaml and are loaded directly
  # to the class when we load the datasource for the first time.
  #
  consul_config = Jerakia.config['consul']
  if consul_config.is_a?(Hash)
    Diplomat.configure do |config|
      config.url = consul_config['url'] if consul_config.has_key?('url')
      config.acl_token = consul_config['acl_token'] if consul_config.has_key?('acl_token')
      config.options = consul_config['options'] if consul_config['options'].is_a?(Hash)
    end
  end

  # Entrypoint for Jerakia lookups starts here.
  #
  def lookup
    key = request.key
    namespace = request.namespace

    Jerakia.log.debug("[datasource::consul_kv] backend performing lookup for namespace:#{namespace[0]} and #{key.nil? ? 'no key' : 'key:' + key}")
    paths = options[:searchpath].reject { |p| p.nil? }

    reply do |response|
      paths.each do |path|
        split_path = path.split('/').reject { |p| p.empty? }

        split_path << namespace
        # Don't append the key if we are parsing values so we can support key
        # lookups with the key inside a value
        split_path << key unless key.nil? || options[:parse_values]

        diplomat_options = {
          :convert_to_hash => options[:to_hash],
          :dc              => options[:datacenter],
          :recurse         => options[:recursive],
        }

        begin
          lookup_path = split_path.flatten.join('/')
          Jerakia.log.debug("[datasource::consul_kv] Looking up #{lookup_path} with options #{diplomat_options}")
          @data = Diplomat::Kv.get(lookup_path, diplomat_options)
          Jerakia.log.debug("Retrieved #{@data} from consul")
        rescue Diplomat::KeyNotFound => e
          Jerakia.log.debug("NotFound encountered, skipping to next path entry")
          next
        rescue Faraday::ConnectionFailed => e
          raise Jerakia::Error, "Failed to connect to consul service: #{e.message}"
          break
        end

        # If we are using recursive and to_hash the result is nested with the path
        # E.g. given a searchpath of common/foo/bar, the resulting hash would be
        # => {"common"=>{"foo"=>{"bar"=>{"a"=>"lorem", "b"=>"ipsum", "c"=>"dolor"}}}}
        # dig will return {"a"=>"lorem", "b"=>"ipsum", "c"=>"dolor"}
        # If we are only using recursive the resulting keys contain the full path
        # E.g. given the same searchpath as above, the resulting array would be
        # => [{:key=>"common/foo/bar/a", :value=>"lorem"}, {:key=>"common/foo/bar/b", :value=>"ipsum"}, {:key=>"common/foo/bar/c", :value=>"dolor"}]
        #
        trim_paths(split_path.flatten)
        Jerakia.log.debug("Trimmed path data is #{@data}")

        parse_data if options[:parse_values]
        Jerakia.log.debug("Parsed data is #{@data}")

        case @data
        when Hash
          if key.nil?
            response.namespace(namespace).submit @data
          else
            if options[:parse_values]
              response.namespace(namespace).key(key).ammend(@data[key])
            else
              response.namespace(namespace).key(key).ammend(@data)
            end
          end
        when Array
          @data.each do |partial_data|
            response.namespace(namespace).key(partial_data[:key]).ammend(partial_data[:value])
          end
        else
          response.namespace(namespace).key(key).ammend(@data) unless key.nil?
        end
      end
    end
  end

  private
  # Helper method to "dig" the keys from result if using :to_hash
  def dig(hash, paths)
    match = hash[paths.shift]
    if paths.empty? or match.nil?
      return match
    else
      return dig(match, paths)
    end
  end

  def parse_data
    # If we are using to_hash (with or without recursive) or
    # not using to_hash and not using recursive
    # pass the data as is.
    # If we are not using to_hash and using recursive,
    # iterate of the results array
    if options[:to_hash]
      if @data.is_a?(Hash)
        @data.each do |key, value|
          @data[key] = parse_value(value)
        end
      else
        @data = parse_value(@data)
      end
    else
      if options[:recursive] and @data.is_a?(Array)
        @data.each {|partial_data| partial_data[:value] = parse_value(partial_data[:value])}
      else
        @data = parse_value(@data)
      end
    end
  end

  def parse_value(value)
    Jerakia.log.debug("Value #{value} is a #{value.class}")
    # If it's a hash and we are using traverse_hash, iterate
    if value.is_a?(Hash)
      if options[:traverse_hash]
        value.each do |sub_key, sub_value|
          Jerakia.log.debug("Parsing sub_value #{sub_value} under sub_key #{sub_key}")
          value[sub_key] = parse_value(sub_value)
        end
      else
        return value
      end
    else
      # Will try HCL, JSON and YAML and if neither works we assume it's raw string
      begin
        parsed_value = HCL::Checker.parse(value)
        Jerakia.log.debug("Data is in HCL format")
      rescue HCLLexer::ScanError
        begin
          parsed_value = JSON.parse(value)
          Jerakia.log.debug("Data is in JSON format")
        rescue JSON::ParserError
          begin
            parsed_value = YAML.load(value)
            Jerakia.log.debug("Data is in YAML format")
          rescue SyntaxError
            parsed_value = value
            Jerakia.log.debug("Data is not in any supported format, leaving as raw string")
          end
        end
      end

      Jerakia.log.debug("Parsed value is #{parsed_value}")
      return parsed_value
    end
  end

  def trim_paths(split_path)
    if options[:to_hash]
      @data = dig(@data, split_path)
      Jerakia.log.debug("Dug data is #{@data}")
    else
      if @data.is_a?(Array)
        @data.each {|partial_data| partial_data[:key] = partial_data[:key].sub!(split_path.join('/') + '/', '')}
      end
    end
  end
end
