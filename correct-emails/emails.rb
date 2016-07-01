require_relative '../config/keys.local.rb'
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
	'sent'=>2,
	'spam'=>1,
	'unknown'=>0,
	'unsub'=>-1,
	'bounced'=>-2,
	'malformed_email'=>-3, # to be checked and changed
	'duplicate'=>-4, # to be checked and changed
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

#a=File.read(ARGV[0])
a=CSV.read(ARGV[0])
update_email_status_and_correct_email="update users set email_status=$3, email=$2 where email=$1 returning *"
update_email_status="update users set email_status=$2 where email=$1 returning *"
check_email="select email from users where email=$1"
errors=[]
emails_to_update=[]
#a.lines do |l|
a.each do |l|
	begin
		query=update_email_status_and_correct_email
		query_params=[l[0],l[1],EMAIL_STATUS['sent']]
		#puts "#{l[0]} => #{l[1]} : #{query_params}"
		res=db.exec_params(query,query_params)
		if res.num_tuples.zero? then
			puts "PASSED email #{l}"
		else
			puts "UPDATED email #{l}"
			emails_to_update.push(l)
		end
	rescue Exception=>e
		STDERR.puts "Exception raised : #{e.message}"
		res=nil
	end
end
db.close()
puts errors
puts "#{emails_to_update.length} emails updated"
exit

