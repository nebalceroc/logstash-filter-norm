Gem::Specification.new do |s|
  s.name          = 'logstash-filter-norm'
  s.version       = '0.1.0'
  s.licenses      = ['Apache License (2.0)']
  s.summary       = 'Rewrite input.'
  s.description   = 'Conect to normalization project with REST and update all attributes and values.'
  s.homepage      = 'http://www.version3.co'
  s.authors       = ['Juan Pablo Arias']
  s.email         = ''
  s.require_paths = ['lib']

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "filter" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", ">= 2.0.0", "<= 5.3"
  s.add_runtime_dependency 'addressable', '= 2.3.8'
  s.add_runtime_dependency 'rest-client', '<= 2.0.2'
  s.add_runtime_dependency 'elasticsearch', '>= 5.0.0'
  s.add_development_dependency 'logstash-devutils'
end
