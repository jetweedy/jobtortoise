# jtapp


## Setup

Copy sample.env to .env and fill in the relevant credentials.

```
cd app

python -m venv .venv

.\.venv\Scripts\activate
## or
source .venv/bin/activate

python -m pip install --upgrade pip

pip install pandas requests flask sqlalchemy pymysql beautifulsoup4 feedparser python-dotenv psycopg2-binary
```

## Run Locally
```
flask run
```

## Run It (on Linux / Remote)

```
docker build --no-cache -t jtapp .

docker run -d -p 5000:5000 -v $(pwd)/data:/app/data -e SQLITE_DB_PATH=/app/data/app.db --user $(id -u):$(id -g) --name jtapp jtapp
```

## Re-run it:

```
docker stop jtapp
docker rm jtapp
docker rmi jtapp
docker build --no-cache -t jtapp .
docker run -d -p 5000:5000 -v $(pwd)/../data:/data -e SQLITE_DB_PATH=/data/app.db --user $(id -u):$(id -g) --name jtapp jtapp
```


### To wipe all images (in case needed):

```
docker rm -f $(docker ps -aq)
docker rmi -f $(docker images -aq)
```

## Deploying to AWS EC2:

Create EC2 Instance (micro-2 free tier should be fine).

When doing so, create anm RSA key pair and save it (jtapp.pem for my examples...)


```
scp -i jtapp.pem -r ./jtapp ec2-user@ec2-XX-XXX-XX-XXX.compute-1.amazonaws.com:/home/ec2-user/jtapp/

ssh -i jtapp.pem ec2-user@ec2-XX-XXX-XX-XXX.compute-1.amazonaws.com

mkdir -p /home/ec2-user/jtapp/data
sudo chown -R ec2-user:ec2-user /home/ec2-user/jtapp
sudo chmod -R u+rwX /home/ec2-user/jtapp
chown ec2-user:ec2-user /home/ec2-user/jtapp/data/app.db
chmod 664 /home/ec2-user/jtapp/data/app.db

sudo dnf update -y
sudo dnf install -y docker
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user

exit

ssh -i jtapp.pem ec2-user@ec2-XX-XXX-XX-XXX.compute-1.amazonaws.com

sudo dnf install -y nginx certbot python3-certbot-nginx
sudo systemctl start nginx
sudo systemctl enable nginx

sudo nano /etc/nginx/conf.d/jtapp.conf

server {
    listen 80;
    server_name jtapp.com www.jtapp.com;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}

sudo nginx -t
sudo systemctl reload nginx

sudo certbot --nginx -d jtapp.com -d www.jtapp.com
```

To re-deploy on EC2 after edits:

```
scp -i jtapp.pem -r ./jtapp ec2-user@ec2-XX-XXX-XX-XXX.compute-1.amazonaws.com:/home/ec2-user/jtapp/

ssh -i jtapp.pem ec2-user@ec2-XX-XXX-XX-XXX.compute-1.amazonaws.com

cd ~/jtapp/jtapp

bash rebuild.sh
```

