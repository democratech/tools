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
EMAIL_STATUS={
	'spam'=>1,
	'sent'=>2,
	'unknown'=>0,
	'soft-bounced'=>-1,
	'bounced'=>-2
}

def fix_wufoo(url)
	url.gsub!(':/','://') if url.match(/https?:\/\//).nil?
	return url
end

def strip_tags(text)
	return text.gsub(/<\/?[^>]*>/, "")
end

db=PG.connect(
	"dbname"=>PGNAME,
	"user"=>PGUSER,
	"password"=>PGPWD,
	"host"=>PGHOST,
	"port"=>PGPORT
)

a=CSV.read(ARGV[0])
update_email_status=<<END
update users set email_status=$2, email_detail=$3 where email=$1 returning *;
END
update_email_status_and_correct_email=<<END
update users set email_status=$2, email_detail=$3, email=$4 where email=$1 returning *;
END
errors=[]
a.each do |l|
	begin
# Date,Email Address,Correction,Sender,Subject,Status,Tags,Subaccount,Opens,Clicks,Bounce Detail
	next if l[0]=='Date'
	v={
		:email=>l[1],
		:email_correction=>l[2],
		:email_status=>EMAIL_STATUS[l[5]],
		:email_detail=>l[10]
	}
	query=update_email_status
	query_params=[v[:email],v[:email_status],v[:email_detail]]
	if not v[:email_correction].nil? then
		query=update_email_status_and_correct_email
		query_params.push(v[:email_correction])
	end
	res=db.exec_params(query,query_params)
	raise "email #{v[:email]} not found" if res.num_tuples.zero?
	#puts "result #{v}"
	#puts "query #{query}"
	#puts "query_params #{query_params}"
	puts "email #{v[:email]} mis à jour !"
	rescue Exception=>e
		errors.push(v)
		STDERR.puts "Exception raised : #{e.message}"
		res=nil
	end
end
db.close()
puts errors
puts "#{errors.length} erreurs found"
exit

