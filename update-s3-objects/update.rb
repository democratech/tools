require_relative '../../config/keys.local.rb'
require 'mimemagic'
require 'open-uri'
require 'aws-sdk'
aws=Aws::S3::Resource.new(
	credentials: Aws::Credentials.new(AWS_BOT_KEY,AWS_BOT_SECRET),
	region: AWS_REGION
)
bucket=aws.bucket(AWS_BUCKET)
=begin
bucket.objects.each do |ob|
	#puts "ob #{ob.inspect}"
	obj=ob.get
	url=ob.public_url
	#puts "obj #{obj.inspect}"
	content_type=obj.content_type
	#puts "meta #{content_type}"
	if content_type.nil? or content_type.empty? then
		content_type=MimeMagic.by_magic(open(url)).type
		puts "updating content_type of #{ob.key} to #{content_type}"
		#ob.copy_to(bucket.object(ob.key), :metadata=>{':Content-Type'=>content_type}, :content_type => content_type)
		#break
	end
	#puts MimeMagic.by_magic(open(ARGV[0])).type


	#break
	#ontent_type = "foo/bar"
	#ob.copy_to(ob.key, :metadata{:foo => metadata[:foo]}, :content_type => content_type)
end
=end
bucket.objects.each do |object|
	url=object.public_url
	k = object.key
	obj=object.get
	content_type=obj.content_type
	puts "analysing #{k} (url #{url})"
	begin
		content_type=MimeMagic.by_magic(open(url)).type
	rescue OpenURI::HTTPError => e
		bucket.object(k).copy_to( "laprimaire/#{k}", :metadata_directive=>'REPLACE', :content_type => content_type, :acl=>'public-read', :cache_control=>'public, max-age=14400')
		puts "object updated"
	end
	#puts "updating content_type of #{k} to #{content_type}"
end
