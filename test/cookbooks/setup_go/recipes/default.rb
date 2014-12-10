directory '/opt/go/src/github.com/sathlan' do
  recursive true
end

link "/opt/go/src/github.com/sathlan/libradosgo" do
  to "/vagrant"
end
