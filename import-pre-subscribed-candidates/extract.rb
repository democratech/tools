require 'csv'
a=CSV.read('photos.csv')
accepted_formats=['.jpg','.jpeg','.png']
a.each do |l|
	uuid=l[0]
	name=l[1]
	next if name.nil?
	extension=File.extname(name).downcase
	next if not accepted_formats.include? extension
	url=l[2].match(/http.*\)/)[0][0..-2]
	puts "wget -O %{name} %{url} && convert %{name} -resize x300 %{name} && mv %{name} photos/%{uuid}%{extension}" % {:name=>name,:uuid=>uuid,:url=>url,:extension=>extension}
end
