import json
import pika
from app.server import Server

def main():
    cfg = json.load('config.json')
    server = Server(cfg)
    result = server.fct_test()
    print(result['status'])
    
if __name__ == "__main__":
    main()
