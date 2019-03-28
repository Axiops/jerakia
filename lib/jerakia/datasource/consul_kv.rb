require 'diplomat'
require 'hcl/checker'
require 'uri'

class Jerakia::Datasource::Consul_kv < Jerakia::Datasource::Instance

  # datacenter option sets the :dc method in the Diplomat
  # request to perform a consul lookup using a particular
  # datacenter
  #
  option(:datacenter) { |str| str.is_a?(String) }

  # to_hash, when used with recursive, will consolidate the
  # results into a hash, instead of an array
  #
  option(:to_hash, :default => true) { |opt|
    [ TrueClass, FalseClass ].include?(opt.class)
  }

  # Recursive will return the entire data structure from consul
  # rather than just the requested key
  #
  option(:recursive, :default => false) { |opt|
    [ TrueClass, FalseClass ].include?(opt.class)
  }

  # The searchpath is the root path of the request, which will
  # get appended with the namespace, and key if applicable.
  # We allow this option to be ommited in case the lookup path
  # starts at the namesapce.
  #
  option(:searchpath, :default => ['']) { |opt| opt.is_a?(Array) }

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

    # Some options combinations don't make sense so fail early
    # We support keyless lookups with
    # - :to_hash and :resursive both true
    # - :to_hash and :resursive both false (the namespace becomes the key and we parse the result)
    # If a key is specified, then it can't be recursive
    #
    if key.nil?
      raise Jerakia::Error, "Invalid combination of options for keyless lookup. :recursive and :to_hash need to both true or false" unless
        (options[:recursive] and options[:to_hash]) or (not options[:recursive] and not options[:to_hash])
    else
      raise Jerakia::Error, "Recursive is only for keyless lookups" if options[:recursive]
    end

    Jerakia.log.debug("[datasource::consul_kv] backend performing lookup for namespace:#{namespace[0]} and #{key.nil? ? 'no key' : 'key:' + key}")
    paths = options[:searchpath].reject { |p| p.nil? }

    reply do |response|
      paths.each do |path|
        split_path = path.split('/').compact

        split_path << namespace
        split_path << key unless key.nil?

        diplomat_options = {
          :recurse => options[:recursive],
          :convert_to_hash => options[:to_hash],
          :dc => options[:datacenter],
        }

        begin
          key_path = split_path.flatten.join('/')
          Jerakia.log.debug("[datasource::consul_kv] Looking up #{key_path} with options #{diplomat_options}")
          data = Diplomat::Kv.get(key_path, diplomat_options)
          Jerakia.log.debug("Retrieved #{data} from consul")
        rescue Diplomat::KeyNotFound => e
          Jerakia.log.debug("NotFound encountered, skipping to next path entry")
          next
        rescue Faraday::ConnectionFailed => e
          raise Jerakia::Error, "Failed to connect to consul service: #{e.message}"
          break
        end

        if options[:to_hash]
          parsed_data = dig(data, split_path.flatten)
        else
          # Try to parse the data if we are not using :to_hash
          # Will try HCL, JSON and YAML and if neither works we assume it's raw string
          begin
            parsed_data = HCL::Checker.parse(data)
            Jerakia.log.debug("Data was in HCL format")
          rescue HCLLexer::ScanError
            begin
              parsed_data = JSON.parse(data)
              Jerakia.log.debug("Data was in JSON format")
            rescue JSON::ParserError
              begin
                parsed_data = YAML.load(data)
                Jerakia.log.debug("Data was in YAML format")
              rescue SyntaxError
                parsed_data = data
                Jerakia.log.debug("Cannot parse data, leaving as raw string")
              end
            end
          end
        end
        Jerakia.log.debug("Parsed data is #{parsed_data}")

        if parsed_data.is_a?(Hash)
          response.namespace(namespace).submit parsed_data
        else
          response.namespace(namespace).key(key).ammend(parsed_data) unless key.nil?
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
end
