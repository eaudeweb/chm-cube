packages = %w{build-essential mongodb}
packages.each do |pkg|
  apt_package pkg do
    action :install
  end
end
