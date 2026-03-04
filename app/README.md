# qrapp


## Setup

Copy sample.env to .env and fill in the relevant credentials.

```
python -m venv .venv

.\.venv\Scripts\activate
## or
source .venv/bin/activate

python -m pip install --upgrade pip

pip install pandas requests flask sqlalchemy pymysql beautifulsoup4 feedparser python-dotenv psycopg2-binary
```





## Run It

```
docker build --no-cache -t qrapp .

docker run -d -p 5000:5000 -v $(pwd)/data:/app/data -e SQLITE_DB_PATH=/app/data/app.db --user $(id -u):$(id -g) --name qrapp qrapp
```

## Re-run it:

```
bash rebuild.sh
```
This runs...
```
docker stop qrapp
docker rm qrapp
docker rmi qrapp
docker build --no-cache -t qrapp .
docker run -d -p 5000:5000 -v $(pwd)/../data:/data -e SQLITE_DB_PATH=/data/app.db --user $(id -u):$(id -g) --name qrapp qrapp
```


### To wipe all images (in case needed):

```
docker rm -f $(docker ps -aq)
docker rmi -f $(docker images -aq)
```

## Deploying to AWS EC2:

Create EC2 Instance (micro-2 free tier should be fine).

When doing so, create anm RSA key pair and save it (qrapp.pem for my examples...)


```
scp -i qrapp.pem -r ./qrapp ec2-user@ec2-XX-XXX-XX-XXX.compute-1.amazonaws.com:/home/ec2-user/qrapp/

ssh -i qrapp.pem ec2-user@ec2-XX-XXX-XX-XXX.compute-1.amazonaws.com

mkdir -p /home/ec2-user/qrapp/data
sudo chown -R ec2-user:ec2-user /home/ec2-user/qrapp
sudo chmod -R u+rwX /home/ec2-user/qrapp
chown ec2-user:ec2-user /home/ec2-user/qrapp/data/app.db
chmod 664 /home/ec2-user/qrapp/data/app.db

sudo dnf update -y
sudo dnf install -y docker
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user

exit

ssh -i qrapp.pem ec2-user@ec2-XX-XXX-XX-XXX.compute-1.amazonaws.com

sudo dnf install -y nginx certbot python3-certbot-nginx
sudo systemctl start nginx
sudo systemctl enable nginx

sudo nano /etc/nginx/conf.d/qrapp.conf

server {
    listen 80;
    server_name qrapp.com www.qrapp.com;

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

sudo certbot --nginx -d qrapp.com -d www.qrapp.com
```

To re-deploy on EC2 after edits:

```
scp -i qrapp.pem -r ./qrapp ec2-user@ec2-XX-XXX-XX-XXX.compute-1.amazonaws.com:/home/ec2-user/qrapp/

ssh -i qrapp.pem ec2-user@ec2-XX-XXX-XX-XXX.compute-1.amazonaws.com

cd ~/qrapp/qrapp

bash rebuild.sh
```

