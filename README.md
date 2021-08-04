# AlphaBroder Shopify Python Integration
API for updating Shopify with AlphaBroder inventory changes.

# CentOS Setup
 - $ yum install -y gcc zlib-devel bzip2 bzip2-devel readline-devel sqlite sqlite-devel openssl-devel tk-devel libffi-devel xz-devel git make tar patch libxslt-devel libxml2-devel
 - $ git clone https://github.com/pyenv/pyenv.git ~/.pyenv
 - $ cd ~/.pyenv && src/configure && make -C src
 - Add 'export PYENV_ROOT="$HOME/.pyenv"' to ~/.bashrc
 - Add 'export PATH="$PYENV_ROOT/bin:$PATH"' to ~/.bashrc
 - Add 'eval "$(pyenv init --path)"' to ~/.bashrc
 - Add 'eval "$(pyenv init -)"' to ~/.bashrc
 - $ pyenv install 3.8.10
 - $ pyenv global 3.8.10 (This is only if no system-wide python version has been set. Not necessary in all cases and be very careful with this command. If you don't understand how this may be a problem, SKIP IT! The rest of the setup may still work.)
 - $ git clone https://github.com/pyenv/pyenv-virtualenv.git $(pyenv root)/plugins/pyenv-virtualenv
 - Add 'eval "$(pyenv virtualenv-init -)"' to ~/.bashrc
 - $ pyenv virtualenv zach-env
 - $ pyenv activate zach-env
 - $ pip install -r requirements.txt
 - Now ready to run: i.e. $ python integration.py -ivcd
