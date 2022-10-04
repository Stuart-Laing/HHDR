#!/usr/bin/env python

__author__ = "Stuart Laing"
__license__ = "GPL"

"""
python 3.10
Proved to work against Heap Dumps from Hadoop Version xxx
"""

import argparse
import json
import os
import hashlib

import requests
from bs4 import BeautifulSoup


def hash_file(file_path: str, algorithm):
    block_size = 65536

    file_hash = algorithm
    with open(file_path, "rb") as f:
        fb = f.read(block_size)
        while len(fb) > 0:
            file_hash.update(fb)
            fb = f.read(block_size)
    return file_hash.hexdigest()


def send_query(url: str, query: str, response_has_ints: bool) -> list[str]:
    r = requests.get(url, f"query={query}")
    soup = BeautifulSoup(r.content.decode(), "html.parser")

    if response_has_ints:
        a = [x.getText().strip() for x in soup.find(border="1").find_all("td")]
        b = []
        for thing in a:
            if thing[-2:] == ".0":
                b.append(thing[:-2])
            elif "E" in thing:
                b.append(thing[:thing.find("E")].replace(".", ""))
            else:
                b.append(thing)
        return b

    return [x.getText().strip() for x in soup.find(border="1").find_all("td")]


def send_queries(url: str, queries: list[tuple[str, bool]]) -> list[list[str]]:
    responses = [send_query(url, *q) for q in queries]
    return responses


def write_data(data, path: str):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def extract_from_namenode(oql_url: str, dump_file: str):
    data = {
        "File Info": {
            "WebServer Launched From Filename": os.path.basename(dump_file),
            "SHA256 Hash": hash_file(dump_file, hashlib.sha256()),
            "MD5 Hash": hash_file(dump_file, hashlib.md5())
        },
        "DataNodes": [],
        "Blocks": [],
        "Files": [],
        "Directories": []
    }

    c = "org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo"
    datanode_queries = [
        (f"select x.dn.datanodeUuid.toString() from {c} x", False),
        (f"select x.storageID.toString() from {c} x", False),
        (f"select x.dn.ipAddr.toString() from {c} x", False),
        (f"select x.dn.hostName.toString() from {c} x", False),
        (f"select x.dn.lastUpdate from {c} x", True),
        (f"select x.dn.location.toString() from {c} x", False)
    ]
    datanodes = send_queries(oql_url, datanode_queries)

    for datanode_index in range(0, len(datanodes[0])):
        data["DataNodes"].append({
            "UUID": datanodes[0][datanode_index],
            "Storage ID": datanodes[1][datanode_index],
            "IP Address": datanodes[2][datanode_index],
            "Hostname": datanodes[3][datanode_index],
            "Last Updated": datanodes[4][datanode_index],
            "Location": datanodes[5][datanode_index]
        })

    c = "org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous"
    block_queries = [
        (f"select x.bcId from {c} x", True),
        (f"select x.blockId from {c} x", True),
        (f"select x.generationStamp from {c} x", True),
        (f"select x.replication from {c} x", True),
        (f"select (x.bcId==-1) ? '[  ]' : map(x.storages, \"it.storageID.toString()\") from {c} x", False)
    ]
    blocks = send_queries(oql_url, block_queries)

    for block_index in range(0, len(blocks[0])):
        data["Blocks"].append({
            "Block Collection ID": blocks[0][block_index],
            "Block ID": blocks[1][block_index],
            "Generation Stamp": blocks[2][block_index],
            "Replication": blocks[3][block_index],
            "Located On": blocks[4][block_index][2:-2].split(", ")
            if blocks[4][block_index] != "[  ]" else ""
        })

    c = "org.apache.hadoop.hdfs.server.namenode.INodeFile"
    file_queries = [
        (f"select x.id from {c} x", True),
        (f"select x.name.toString() from {c} x", False),
        (f"select x.parent.id from {c} x", True),
        (f"select x.modificationTime from {c} x", True),
        (f"select map(x.blocks, \"it.blockId\") from {c} x", False)
    ]
    files = send_queries(oql_url, file_queries)

    for file_index in range(0, len(files[0])):
        data["Files"].append({
            "ID": files[0][file_index],
            "Name": "".join([chr(int(c, 16)) for c in files[1][file_index][1:-1].split(", ")]),
            "Parent Directory ID": files[2][file_index],
            "Modification Time": files[3][file_index],
            "Blocks": files[4][file_index][2:-2].split(", ")
        })

    c = "org.apache.hadoop.hdfs.server.namenode.INodeDirectory"
    directory_queries = [
        (f"select x.id from {c} x", True),
        (f"select x.name.toString() from {c} x", False),
        (f"select (x.parent==null) ? 'null' : x.parent.id from {c} x", True)
    ]
    directories = send_queries(oql_url, directory_queries)

    for directory_index in range(0, len(directories[0])):
        data["Directories"].append({
            "ID": directories[0][directory_index],
            "Name": "".join([chr(int(c, 16)) for c in directories[1][directory_index][1:-1].split(", ")])
            if directories[1][directory_index] != "{}" else "null",
            "Parent Directory ID": directories[2][directory_index]
        })

    return data


def extract_from_datanode(oql_url: str, dump_file: str):
    data = {
        "File Info": {
            "WebServer Launched From Filename": os.path.basename(dump_file),
            "SHA256 Hash": hash_file(dump_file, hashlib.sha256()),
            "MD5 Hash": hash_file(dump_file, hashlib.md5())
        },
        "DataNode": {},
        "Blocks": []
    }

    c = "org.apache.hadoop.hdfs.server.datanode.DataNode"
    datanode_queries = [
        (f"select x.clusterId.toString() from {c} x", False),
        (f"select x.id.datanodeUuid.toString() from {c} x", False),
        (f"select x.id.hostName.toString() from {c} x", False),
        (f"select x.id.ipAddr.toString() from {c} x", False)
    ]
    datanode = send_queries(oql_url, datanode_queries)

    data["DataNode"] = {
        "Cluster ID": datanode[0][0],
        "UUID": datanode[1][0],
        "Hostname": datanode[2][0],
        "Ip Address": datanode[3][0]
    }

    c = "org.apache.hadoop.hdfs.server.datanode.FinalizedReplica"
    block_queries = [
        (f"select x.blockId from {c} x", True),
        (f"select x.numBytes from {c} x", True),
        (f"select x.generationStamp from {c} x", True),
        (f"select x.baseDir.path.toString() from {c} x", False)
    ]
    blocks = send_queries(oql_url, block_queries)

    for block_index in range(0, len(blocks[0])):
        data["Blocks"].append({
            "Block ID": blocks[0][block_index],
            "Number of Bytes": blocks[1][block_index],
            "Generation Stamp": blocks[2][block_index],
            "Full Path On Node": blocks[3][block_index]
        })

    return data


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("-u", "--url", dest="url", type=str,
                        required=True, help="The url and port that the heap dump is hosted on")
    parser.add_argument("-d", "--dump-file", dest="dump_file", type=str,
                        required=True, help="The heap dump that the webserver is launched from")
    parser.add_argument("-t", "--type", dest="type", type=str,
                        required=True, help="The type of the heap dump. Either 'NameNode' or 'DataNode'")
    parser.add_argument("OUT_PATH", type=str,
                        help="The file that the json formatted results will be saved into")

    args = parser.parse_args()

    url: str = args.url
    dump_file: str = args.dump_file
    oql_url: str = f"{url}/oql/"
    dump_type: str = args.type
    out_path: str = args.OUT_PATH

    print(f"Extracting {dump_type} data from {oql_url}")

    try:
        requests.get(url)
    except requests.exceptions.ConnectionError:
        print(f"Connection Error to {url}, ensure webserver is launched and URL includes protocol")
        exit()

    if dump_type.lower() == "namenode":
        data = extract_from_namenode(oql_url, dump_file)

    elif dump_type.lower() == "datanode":
        data = extract_from_datanode(oql_url, dump_file)

    else:
        print("Type error")
        exit()

    write_data(data, out_path)
    print(f"Data successfully written to output file '{out_path}'")


if __name__ == '__main__':
    main()
