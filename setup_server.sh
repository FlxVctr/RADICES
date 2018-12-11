wget https://raw.githubusercontent.com/chetankapoor/swap/master/swap.sh -O swap.sh
sudo sh swap.sh 2G
sudo apt-get update
sudo apt-get install mysql-server
mysql_secure_installation
sudo mysql -u root -p
create database sparsetwitter;
create database sparsetwitter_live;
set global validate_password_special_char_count = 0;
create user 'sparsetwitter'@'localhost' identified by 'password';
GRANT ALL PRIVILEGES ON *.* TO 'sparsetwitter'@'localhost';
create user 'sparsetwitter_remote'@'%' identified by 'password';
GRANT ALL PRIVILEGES ON sparsetwitter.* TO 'sparsetwitter_remote'@'%';
GRANT ALL PRIVILEGES ON sparsetwitter_live.* TO 'sparsetwitter_remote'@'%';
# follow this guide: https://medium.com/@haotangio/how-to-properly-setup-mysql-5-7-for-production-on-ubuntu-16-04-dd4088286016
exit
git clone https://github.com/FlxVctr/SparseTwitter.git
sudo apt install python-pip
pip install --user pipenv 
sudo apt upgrade
sudo reboot now
# ssh back into machine
cd SparseTwitter
screen
curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash
# add stuff to bashrc as prompted by script
source ../.bashrc
pyenv update
sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev \
libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev \
xz-utils tk-dev libffi-dev liblzma-dev
sudo reboot now
# ssh back into machine
screen
cd SparseTwitter
pipenv install
pipenv shell
# follow readme in SparseTwitter
python tests/tests.py -s
python functional_test.py