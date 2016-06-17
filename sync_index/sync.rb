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
index_candidats=Algolia::Index.new("candidates")
index_citoyens=Algolia::Index.new("citizens")
index_search=Algolia::Index.new("search")
candidates_list=<<END
SELECT ca.candidate_id,ca.user_id,ca.name,ca.gender,ca.birthday,ca.job,ca.departement,ca.secteur,ca.accepted,ca.verified,ca.date_added::DATE as date_added,date_part('day',now()-ca.date_added) as nb_days_added,ca.date_verified::DATE as date_verified,date_part('day',now() - ca.date_verified) as nb_days_verified,ca.qualified,ca.date_qualified,ca.official,ca.date_officialized,ca.vision,ca.prio1,ca.prio2,ca.prio3,ca.photo,ca.trello,ca.website,ca.twitter,ca.facebook,ca.youtube,ca.linkedin,ca.tumblr,ca.blog,ca.wikipedia,ca.instagram, z.nb_views, z.nb_soutiens, w.nb_soutiens_7j
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
LEFT JOIN (
	SELECT y.candidate_id, y.nb_views, count(s.user_id) as nb_soutiens_7j
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
	WHERE s.support_date> (now()::date-7)
	GROUP BY y.candidate_id,y.nb_views
) as w
ON (w.candidate_id = ca.candidate_id)
ORDER BY z.nb_soutiens DESC
END

sitemap=<<END
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
END

res=db.exec(candidates_list)
if not res.num_tuples.zero? then
	res.each do |r|
		qualified = r['qualified'].to_b ? "oui" : "non"
		verified = r['verified'].to_b ? "verified" : "not_verified"
		official= r['official'].to_b ? "official" : "not_official"
		gender= r['gender']=='M' ? "Homme" : "Femme"
		birthday=Date.parse(r['birthday'].split('?')[0]) unless r['birthday'].nil?
		status="incomplete"
		unless r['vision'].nil? or r['vision'].empty? then
			status="complete"
		end
		age=nil
		unless birthday.nil? then
			now = Time.now.utc.to_date
			age = now.year - birthday.year - ((now.month > birthday.month || (now.month == birthday.month && now.day >= birthday.day)) ? 0 : 1)
		end

		if (r['verified'].to_b and not r['vision'].nil?) then
			index_candidats.save_object({
				"objectID"=>r['candidate_id'],
				"candidate_id"=>r['candidate_id'],
				"name"=>r['name'],
				"photo"=>r['photo'],
				"gender"=>gender,
				"age"=>age,
				"job"=>r['job'],
				"secteur"=>r['secteur'],
				"departement"=>r['departement'],
				"vision"=>r['vision'],
				"prio1"=>r['prio1'],
				"prio2"=>r['prio2'],
				"prio3"=>r['prio3'],
				"trello"=>r['trello'],
				"website"=>r['website'],
				"twitter"=>r['twitter'],
				"facebook"=>r['facebook'],
				"youtube"=>r['youtube'],
				"linkedin"=>r['linkedin'],
				"tumblr"=>r['tumblr'],
				"blog"=>r['blog'],
				"wikipedia"=>r['wikipedia'],
				"instagram"=>r['instagram'],
				"date_added"=>r['date_added'],
				"nb_days_added"=>r['nb_days_added'].to_i,
				"verified"=>verified,
				"date_verified"=>r['date_verified'],
				"nb_days_verified"=>r['nb_days_verified'].to_i,
				"qualified"=>qualified,
				"date_qualified"=>r['date_qualified'],
				"official"=>official,
				"date_officializied"=>r['date_officializied'],
				"nb_soutiens"=>r['nb_soutiens'].to_i,
				"nb_soutiens_7j"=>r['nb_soutiens_7j'].to_i,
				"nb_views"=>r['nb_views'].to_i,
				"status"=>status
			})
			index_search.save_object({
				"objectID"=>r['candidate_id'],
				"candidate_id"=>r['candidate_id'],
				"name"=>r['name'],
				"photo"=>r['photo'],
				"level"=>3
			})
			sitemap+=<<END
<url>
	<loc>https://laprimaire.org/candidat/#{r['candidate_id']}</loc>
	<lastmod>#{r['date_verified']}</lastmod>
</url>
END
			puts "Added candidat #{r['name']}"
		elsif (r['nb_soutiens'].to_i>1 and not r['verified'].to_b and not r['accepted'].to_b)
			index_citoyens.save_object({
				"objectID"=>r['candidate_id'],
				"candidate_id"=>r['candidate_id'],
				"name"=>r['name'],
				"photo"=>r['photo'],
				"gender"=>gender,
				"date_added"=>r['date_added'],
				"nb_days_added"=>r['nb_days_added'].to_i,
				"nb_soutiens"=>r['nb_soutiens'].to_i,
				"nb_soutiens_7j"=>r['nb_soutiens_7j'].to_i,
				"nb_views"=>r['nb_views'].to_i
			})
			index_search.save_object({
				"objectID"=>r['candidate_id'],
				"candidate_id"=>r['candidate_id'],
				"name"=>r['name'],
				"photo"=>r['photo'],
				"level"=>2
			})
			sitemap+=<<END
<url>
	<loc>https://laprimaire.org/candidat/#{r['candidate_id']}</loc>
	<lastmod>#{r['date_added']}</lastmod>
</url>
END
			puts "Added citoyen #{r['name']}"
		else
			index_search.save_object({
				"objectID"=>r['candidate_id'],
				"candidate_id"=>r['candidate_id'],
				"name"=>r['name'],
				"photo"=>r['photo'],
				"level"=>1
			})
			puts "Skipped citoyen #{r['name']}"
		end
	end
	sitemap+="</urlset>\n"
end
File.write(ARGV[0],sitemap)
