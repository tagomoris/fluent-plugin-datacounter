# -*- encoding: utf-8 -*-
Gem::Specification.new do |gem|
  gem.name        = "fluent-plugin-datacounter"
  gem.version     = "0.4.5"
  gem.authors     = ["TAGOMORI Satoshi"]
  gem.email       = ["tagomoris@gmail.com"]
  gem.homepage    = "https://github.com/tagomoris/fluent-plugin-datacounter"
  gem.summary     = %q{Fluentd plugin to count records with specified regexp patterns}
  gem.description = %q{To count records with string fields by regexps (To count records with numbers, use numeric-counter)}
  gem.license     = "Apache-2.0"

  gem.files         = `git ls-files`.split("\n")
  gem.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.require_paths = ["lib"]

  gem.add_runtime_dependency "fluentd", [">= 0.14.8", "< 2"]

  gem.add_development_dependency "bundler"
  gem.add_development_dependency "rake"
  gem.add_development_dependency "test-unit"
  gem.add_development_dependency "delorean"
end
