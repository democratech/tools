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
index_villes=Algolia::Index.new("villes")
all_cities_list=<<END
select z.population, z.lon_deg,z.lat_deg,z.city_id, z.zipcode, z.slug, count(*) as nb_supporters,(100*(count(*)::float/z.population::float))::numeric(5,3) as taux from (select city_id, zipcode, slug,lon_deg,lat_deg,population from cities group by slug,lon_deg,lat_deg,population,zipcode,city_id) as z inner join citizens as c on (c.city_id=z.city_id) group by z.city_id,z.zipcode, z.slug, z.lat_deg, z.lon_deg, z.population;
END
big_cities_list=<<END
select z.population, z.lon_deg,z.lat_deg,z.city_ids, z.zipcodes, z.slug, count(*) as nb_supporters,(100*(count(*)::float/z.population::float))::numeric(5,3) as taux from (select array_agg(cities.city_id) as city_ids, array_agg(zipcode) as zipcodes, slug,lon_deg,lat_deg,population from cities group by slug,lon_deg,lat_deg,population) as z inner join citizens as c on (array_length(z.city_ids,1)>1 AND c.city_id::int = ANY (z.city_ids::int[])) group by z.city_ids,z.zipcodes, z.slug, z.lat_deg, z.lon_deg, z.population;
END
updates=[]
res=db.exec(all_cities_list)
if not res.num_tuples.zero? then
	res.each do |r|
		updates.push({
			"_geoloc"=>{"lat"=>r['lat_deg'],"lng"=>r['lon_deg']},
			"nb_supporters"=>r['nb_supporters'],
			"taux"=>r['taux'],
			"objectID"=>r['city_id']
		})
		puts "updating #{r['slug']}"
	end
end
index_villes.partial_update_objects(updates)
updates=[]
res=db.exec(big_cities_list)
if not res.num_tuples.zero? then
	res.each do |r|
		object_ids=eval(r['city_ids'].gsub('{','[').gsub('}',']'))
		zipcodes=eval(r['zipcodes'].gsub('{','[').gsub('}',']'))
		zipcodes_text=zipcodes.join(', ')
		first=object_ids[0]
		puts "using #{first} as first"
		object_ids.each do |a|
			puts "deleting #{a}" if a!=first
			index_villes.delete_object(a) if a!=first
		end
		updates.push({
			"zipcode"=>zipcodes_text,
			"_geoloc"=>{"lat"=>r['lat_deg'],"lng"=>r['lon_deg']},
			"nb_supporters"=>r['nb_supporters'],
			"taux"=>r['taux'],
			"objectID"=>first.to_s
		})
	end
end
index_villes.partial_update_objects(updates)
