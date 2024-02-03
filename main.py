from fastapi import FastAPI, Query
from hdfs import InsecureClient
import requests
import os
app = FastAPI()

@app.get("/", status_code=403)
async def root():
    return {"status_code": 403, "msg": "root node access is forbidden"}

@app.get("/process")
async def process(id: str = Query(None, description="查分器id(水鱼查分器为用户名,落雪查分器暂未适配)")):
    # 连接HDFS(Hadoop分布式文件系统)
    client = InsecureClient(url="[hdfs_url]", user="root")
    # 设置水鱼服务器地址
    url = f"https://www.diving-fish.com/api/maimaidxprober/dev/player/records?username={id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; ElecOS3.0; x64) BMSGIntiService/3.6 (BigDataEngine6.0;HDFS3;HDFS;Hadoop) MaiBigData/1.0.5",
        "Developer-Token": "[Token is Private]"
    }
    # 读取数据
    response = requests.get(url, headers=headers)
    # 判断响应码
    if response.status_code == 200:
        # 解析Json
        data = response.json()
        records = data["records"]
        filename = f"{data['nickname']}.txt"
        # 容灾清理
        if os.path.exists(filename):
            os.remove(filename)
        file = open(filename, 'a', encoding='utf-8')
        # 写入数据到本地(临时存储)
        for title in records:
            songname = "《"+''.join(title['title']) + "》\n"
            file.write(songname)
        if not os.path.getsize(filename):
            file = open(filename, 'a', encoding='utf-8')
            for title in records:
                songname = "《"+''.join(title['title']) + "》\n"
                file.write(songname)
        # 判断HDFS中是否存在同名文件
        if client.content(hdfs_path=f"/linkis/maibigdata/input/{filename}", strict=False) is not None:
            # 如果有 删除文件
            client.delete(f"/linkis/maibigdata/input/{filename}")
        # 将文件上传到HDFS
        client.upload(f"/linkis/maibigdata/input/{filename}",f"./{filename}")
        # 关闭文件
        file.close()
        # 清理文件
        os.remove(filename)
        # 回复成功
        return {"status": 200, "msg": "success"}

    else:
        # 判断是用户不存在还是水鱼的服务器崩了
        # 毕竟再好的服务器 也有土豆熟了的时候
        return {"status": -404, "msg": "no such user"} if response.status_code == 400 else {"status": -1600, "msg": "Diving Fish's potato server crashed"}
