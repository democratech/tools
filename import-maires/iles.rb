require_relative '../../config/keys.local.rb'
require 'csv'
require 'pg'
require 'mini_magick'
require 'aws-sdk'
require 'mimemagic'

if ARGV[0].nil? then
	puts "missing file"
	exit 
end
DEBUG=false
PGPWD=DEBUG ? PGPWD_TEST : PGPWD_LIVE
PGNAME=DEBUG ? PGNAME_TEST : PGNAME_LIVE
PGUSER=DEBUG ? PGUSER_TEST : PGUSER_LIVE
PGHOST=DEBUG ? PGHOST_TEST : PGHOST_LIVE

def fix_wufoo(url)
	url.gsub!(':/','://') if url.match(/https?:\/\//).nil?
	return url
end

def strip_tags(text)
	return text.gsub(/<\/?[^>]*>/, "")
end

def upload_image(filename)
	aws=Aws::S3::Resource.new(
		credentials: Aws::Credentials.new(AWS_BOT_KEY,AWS_BOT_SECRET),
		region: AWS_REGION
	)
	bucket=aws.bucket(AWS_BUCKET)
	key=File.basename(filename)
	obj=bucket.object(key)
	if bucket.object(key).exists? then
		STDERR.puts "#{key} already exists in S3 bucket. deleting previous object."
		obj.delete
	end
	content_type=MimeMagic.by_magic(File.open(filename)).type
	obj.upload_file(filename, acl:'public-read',cache_control:'public, max-age=14400', content_type:content_type)
	return key
end

db=PG.connect(
	"dbname"=>PGNAME,
	"user"=>PGUSER,
	"password"=>PGPWD,
	"host"=>PGHOST,
	"port"=>PGPORT
)

a=CSV.read(ARGV[0])
update_city=<<END
update cities set code_departement=$2, mayor_lname=$3, mayor_fname=$4, mayor_gender=$5, mayor_bday=$6, mayor_job_code=$7, mayor_job_name=$8, name_departement=$9 where num_commune=$1 and departement=$10 returning *
END
#name,name_maj,zipcode,population,name_departement,code_departement,departement,num_commune
insert_city=<<END
insert into cities (name,slug,zipcode,population,name_departement,code_departement,departement,num_commune) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) returning *
END
begin
a.each do |l|
	next if l[0]=='name'
	res=db.exec_params(insert_city,[l[0],l[0].downcase.gsub(' ','-'),l[2],l[3],l[4],l[5],l[6],l[7]])
	raise "city #{l[0]} could not be added" if res.num_tuples.zero?
	puts "city #{l[0]} ajoutée !"
end
rescue Exception=>e
	STDERR.puts "Exception raised : #{e.message}"
	res=nil
ensure
	db.close()
end
exit
begin
	# 1. Enregistrement du candidat
	uuid=((rand()*1000000000000).to_i).to_s
	profile_pic=nil
	if not p[:photo].nil? and not p[:photo].empty? then
		profile_pic="#{uuid}"+File.extname(p[:photo])
		photo=profile_pic
		upload_img=MiniMagick::Image.open(p[:photo_url])
		upload_img.resize "x300"
		photo_path="/tmp/#{photo}"
		upload_img.write(photo_path)
		upload_image(photo_path)
	end
	maj={
		:candidate_id => uuid,
		:name => fix_wufoo(strip_tags(p[:firstname]+' '+p[:lastname])),
		:gender => p[:gender]=="Un homme" ? "M" : "F",
		:country => p[:france]=="Oui" ? "FRANCE" : p[:country],
		:zipcode =>  p[:france]=="Oui" ? fix_wufoo(strip_tags(p[:zipcode])) : nil,
		:email => fix_wufoo(strip_tags(p[:email])),
		:job => fix_wufoo(strip_tags(p[:job])),
		:tel => fix_wufoo(strip_tags(p[:tel])),
		:program_theme => p[:programme]=="Un programme complet" ? "global" : fix_wufoo(strip_tags(p[:theme])),
		:with_team => p[:team].match(/seul/).nil?,
		:political_party => p[:parti]=="Oui" ? fix_wufoo(strip_tags(p[:parti_name].upcase)) : "NON",
		:already_candidate => p[:election].match(/Non/).nil? ? fix_wufoo(strip_tags(p[:election_name].upcase)) : "NON",
		:already_elected => p[:mandat]=="Oui" ? fix_wufoo(strip_tags(p[:mandat_name].upcase)) : "NON",
		:website => fix_wufoo(strip_tags(p[:website])),
		:twitter => fix_wufoo(strip_tags(p[:twitter])),
		:facebook => fix_wufoo(strip_tags(p[:facebook])),
		:photo_key => profile_pic,
	}
	insert_candidate=<<END
INSERT INTO candidates (candidate_id,name,gender,country,zipcode,email,job,tel,program_theme,with_team,political_party,already_candidate,already_elected,website,twitter,facebook,photo,candidate_key) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,md5(random()::text)) RETURNING *
END
	res=db.exec_params(insert_candidate,[
		maj[:candidate_id],
		maj[:name],
		maj[:gender],
		maj[:country],
		maj[:zipcode],
		maj[:email],
		maj[:job],
		maj[:tel],
		maj[:program_theme],
		maj[:with_team],
		maj[:political_party],
		maj[:already_candidate],
		maj[:already_elected],
		maj[:website],
		maj[:twitter],
		maj[:facebook],
		maj[:photo_key]
	])
	raise "candidate could not be created" if res.num_tuples.zero?
	puts "candidat #{maj[:name]} importé !"
rescue Exception=>e
	STDERR.puts "Exception raised : #{e.message}"
	res=nil
ensure
	db.close()
end
