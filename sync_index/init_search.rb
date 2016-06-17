require_relative '../config/keys.local.rb'
require 'csv'
require 'uri'
require 'net/http'
require 'json'
require 'pg'
require 'openssl'
require 'algoliasearch'
require 'wannabe_bool'

db=PG.connect(
	"dbname"=>PGNAME,
	"user"=>PGUSER,
	"password"=>PGPWD,
	"host"=>PGHOST,
	"port"=>PGPORT
)
Algolia.init :application_id=>ALGOLIA_ID, :api_key=>ALGOLIA_KEY
index_search=Algolia::Index.new("search")
candidates_list=<<END
SELECT ca.candidate_id,ca.user_id,ca.name,ca.gender,ca.verified,ca.date_added,date_part('day',now()-ca.date_added) as nb_days_added,ca.date_verified,date_part('day',now() - ca.date_verified) as nb_days_verified,ca.qualified,ca.date_qualified,ca.official,ca.date_officialized,ca.photo,ca.trello,ca.website,ca.twitter,ca.facebook,ca.youtube,ca.linkedin,ca.tumblr,ca.blog,ca.wikipedia,ca.instagram, z.nb_views, z.nb_soutiens
FROM candidates as ca
LEFT JOIN (
       SELECT y.candidate_id, y.nb_views, count(s.user_id) as nb_soutiens
 FROM (
		SELECT c.candidate_id, sum(cv.nb_views) as nb_views
		  FROM candidates as c
		  LEFT JOIN candidates_views as cv
		    ON (
			       cv.candidate_id=c.candidate_id
		       )
		 GROUP BY c.candidate_id
	) as y
	LEFT JOIN supporters as s
	ON ( s.candidate_id=y.candidate_id)
	GROUP BY y.candidate_id,y.nb_views
) as z
ON (z.candidate_id = ca.candidate_id)
END

res=db.exec(candidates_list)
if not res.num_tuples.zero? then
	res.each do |r|
		index_search.save_object({
			"objectID"=>r['candidate_id'],
			"candidate_id"=>r['candidate_id'],
			"name"=>r['name'],
			"photo"=>r['photo']
		})
		puts "Added candidat #{r['name']}"
	end
end
