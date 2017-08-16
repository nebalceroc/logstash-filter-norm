# encoding: utf-8
require 'logstash/filters/base'
require 'logstash/namespace'
require 'json'
require "addressable/uri"
require 'rest-client'
require 'elasticsearch'
require 'digest/sha1'
require 'date'

module Enumerable
  def avg_stddev
    return nil unless count > 0
    return [ first, 0 ] if count == 1
    sx = sx2 = 0
    each do |x|
      sx2 += x**2
      sx += x
    end
    [sx.to_f/count,Math.sqrt((sx2 - sx**2.0/count)/(count - 1))]
  end
end

# This  filter will replace the contents of the default
# message field with whatever you specify in the configuration.
#
# It is only intended to be used as an .
class LogStash::Filters::Norm < LogStash::Filters::Base

    # Setting the config_name here is required. This is how you
    # configure this filter from your Logstash config.
    #
    # filter {
    #    norm {
    #     url_norm => "My message..."
    #   }
    # }
    #
    config_name 'norm'

    # Remote host with norm server
    config :url_norm, validate: :string, required: true, default: 'http://localhost:8080'

    # Remote host with es2 service
    config :url_es2, validate: :string, required: true, default: 'http://localhost:8080'

    # Remote host with es2 service
    config :index, validate: :string, required: true, default: 'comparando'

    # Sub ulr of categories
    config :url_category, validate: :string, default: '/api/categories/'

    # Sub ulr of attributes
    config :url_attribute, validate: :string, default: '/api/attributes/'

    # Sub ulr of attributes values
    config :url_values, validate: :string, default: '/api/valueattribute/'

    # token for django conection
    config :token, validate: :string, required: true

    private
    def norm_healloop
      register()
    end

    private
    def es2_healloop
      register()
    end

    public
    def register
      @filter_attr = {}
      # Get categories
      begin
        rc = RestClient.get(url_norm + url_category, headers={'Authorization'=> 'Token ' + token})
        #rc = RestClient::Request.execute(:method => :get, :url => url_norm + url_category, :timeout => 100, :open_timeout => 100, :headers => {'Authorization'=> 'Token ' + token})
        categories = JSON.parse(rc.body)
      rescue JSON::ParserError => e
        @logger.info("norm heal loop triggered by; JSON parsing error")
        @logger.info(e.response.body)
        sleep(5)
        norm_healloop()
      end
      current_time = DateTime.now
      index_date = current_time.strftime(index)
      # Create index comparandoco-Y.m.d
      #url_index = url_es2 + index_date
      url_index = url_es2

      #es = Elasticsearch::Client.new host: url_es2

      #begin

        #response = RestClient.put(url_index, '{}', :content_type => 'text/plain')
        #esponse = RestClient::Request.execute(:method => :put, :url => url_index, :timeout => 100, :open_timeout => 100, :headers => {'content_type' => 'text/plain'})
      #rescue RestClient::BadRequest => e
        #@logger.info("es2 loop triggered by; BadRequest error")
      #  @logger.info(e.response.body)
      #  sleep(5)
        #es2_healloop()
      #end
      #url_es2_mapping = url_index + '/logs/_mapping/'
      # Get attributes
      categories.each do | category |
          cat = category['name']
          # Get attributes for this category
          urla = Addressable::URI.parse(url_norm + url_attribute + cat + "/").normalize.to_str
          ra = RestClient.get(urla, headers={'Authorization'=> 'Token ' + token})
          attributes = JSON.parse(ra.body)
          @filter_attr[cat] = {}
          if attributes.any?
              attributes.each do | attribute |
                  @filter_attr[cat][attribute['name']] = {}
                  # Build attributes alias
                  @filter_attr[cat][attribute['name']]['attralias'] = attribute['alias']
                  # Get all val attributes for this category
                  urlv = Addressable::URI.parse(url_norm + url_values + cat + "/" + attribute['name'] + "/").normalize.to_str
                  rv = RestClient.get(urlv, headers={'Authorization'=> 'Token ' + token})
                  val_attrs = JSON.parse(rv.body)
                  # Build attributes values
                  @filter_attr[cat][attribute['name']]['valattr'] = []
                  if val_attrs.any?
                    @filter_attr[cat][attribute['name']]['valattr'] = {}
                    val_attrs.each do | val_attr |
                      @filter_attr[cat][attribute['name']]['valattr'][val_attr['name']] = val_attr['alias']
                    end
                  end
                  # PUT
                  @logger.info('{"properties" : {"%s" : {"type" : "string","index": "not_analyzed"}}}' % [attribute['name']])
                  #begin
                  #  RestClient.put(url_es2_mapping, '{"properties" : {"%s" : {"type" : "string","index": "not_analyzed"}}}' % [attribute['name']], :content_type => 'text/plain')
                  #rescue RestClient::BadRequest => e
                  #  @logger.info(e.response.body)
                  #end
              end
          end
      end
    end # def register

    private

    def replace(event, attributes, attribute_name=nil, replace_attr = false, replace_val_attr = false)
        # attributes = {"attribute", "possible values separated with comma"}
        attributes.each do |to_replace, replace_with_comma|
            replace_with_comma_list = replace_with_comma.split(';')
            next unless event.include? 'atributos'
            event.get('atributos').clone.each do |key, value|
                if replace_with_comma_list.include?(key) && replace_attr
                  tmp_dict = event.get('atributos')
                  tmp_dict.delete(to_replace)
                  new_attr = "cmp_" + to_replace.downcase
                  @logger.info(new_attr)
                  tmp_dict[new_attr] = value
                  event.set('atributos', tmp_dict)
                end
                if replace_with_comma_list.include?(value) && replace_val_attr
                  if key == attribute_name
                    tmp_dict = event.get('atributos')
                    tmp_dict[key] = to_replace.downcase
                    event.set('atributos', tmp_dict)
                  end
                end
            end
        end
    end

    private

    def replace_attributes(event, attributes)
        replace(event, attributes, attribute_name="", replace_attr = true)
    end

    private

    def replace_val_attributes(event, values_attributes, attribute_name)
        replace(event, values_attributes, attribute_name=attribute_name, replace_attr = false, replace_val_attr = true)
    end

    public

    def filter(event)
        # event.cancel if !event["categoria"]
        if @filter_attr[event.get("categoria")] && event.get('atributos')
          atributos_keys = []
          event.get('atributos').each do |k,v|
            atributos_keys.push(k)
          end
          if (atributos_keys & ['Marca','marca']).empty?
            if event.include? 'marca'
              event.get('atributos')['Marca'] = event.get('marca')
            end
          end
          filter_attr_event = @filter_attr[event.get("categoria")]
          filter_attr_event.each do |attr_name, vals_alias|
              replace_attributes(event, {attr_name => vals_alias['attralias']})
              vals_alias['valattr'].each do |val, val_alias|
                replace_val_attributes(event, {val => val_alias}, attr_name)
              end
          end
        end
        # Set in event.get('precio') the minor value of precios attributes list
        if event.get("precios").kind_of?(Array)
          min_price = 100000000000
          event.get('precios').each do |price|
            if price != nil and price != 0 and price.is_a? Numeric and price < min_price
              min_price = price
            end
            if price.is_a? String
              @logger.error("Not is number: " + price)
              @logger.error("For url: " + event.get('url'))
            end
          end
          if min_price == 100000000000 || event.get('precios').length  == 0
            min_price = nil
          end
          event.set("precio", min_price)
        else
          event.set("precio", nil)
        end


        historial = Array.new
        if event.get('precios_historico').kind_of?(Array)
          event.get('precios_historico').each do |registro_precios|
            if registro_precios.kind_of?(Array)
              min_price = 100000000000
              registro_precios.each do |price|
                if price != nil and price != 0 and price.is_a? Numeric and price < min_price
                  min_price = price
                end
                if price.is_a? String
                  @logger.error("Not is number: " + price)
                  @logger.error("For url: " + event.get('url'))
                end
              end
              if min_price != 100000000000 && registro_precios.length  > 0 && min_price >= 0
                historial << min_price
              end
            end
          end
        end
        if historial.length > 0
          event.set('variabilidad',historial.avg_stddev[1]/historial.avg_stddev[0])
        end


        # Move event.get('atributos') to event
        if event.get('atributos')
          event.get('atributos').each do |key,value|
            event.set(key, value)
          end
          # event.set('atributos', nil)
        end
        if event.get('url')
          urlmd5 = Digest::MD5.hexdigest event.get('url')
          event.set('urlmd5', urlmd5)
          # event.set('atributos', nil)
        end
        # filter_matched should go in the last line of our successful code
        filter_matched(event)
    end # def filter
end # class LogStash::Filters::Norm
