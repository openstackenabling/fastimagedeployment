#!/usr/bin/env python
#
#    Copyright (C) 2013 Intel Corporation.  All rights reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

# coding=utf-8
# UDP Client - udpclient.py
from ctypes import cdll
import socket, os, fcntl, struct
from nova import log as logging

LOG = logging.getLogger(__name__)

class MUdpClient(object):
    """
    Client using UDP to fetching VM image
    """

    def __init__(self, libpath, address, path, token, id, dir):
        self.librarypath = libpath
        self.serverAddr = (address, 10000)
        self.serverIP = address
        self.filePath = path
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.glancetoken = token
        self.imageID = id
        self.fileName = id
        self.outDir = dir

    def get_ip_address(self, ifname):
        """
        Get ip address
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))[20:24])

    def copyFile(self, source, targetDir, file):
        """
        Copy file from source to target dir
        """
        target = os.path.join(targetDir, file)
        if os.path.isfile(source):
            if not os.path.exists(targetDir):
                os.makedirs(targetDir)
            if not os.path.exists(target):
                open(target, "wb").write(open(source, "rb").read())
        return target

    def start(self):
        """
        Start to receive the image
        """

        data = self.glancetoken + "," + self.filePath + "," + self.imageID
        self.sock.sendto(data, self.serverAddr)
        return self.sock.recvfrom(100)

    def fetchImage(self):
        """
        Fetch image
        """
        localip = self.get_ip_address('eth0')
        if not cmp(localip, self.serverIP):
            return self.copyFile(self.filePath, self.outDir, self.fileName)
        retMsg, address = self.start()
        para = retMsg.split(",")
        receiverClient = cdll.LoadLibrary(self.librarypath)
        LOG.info("file name:"+self.fileName)
        receiverClient.start_receiver_client(long(para[0]), para[1], int(para[2]), self.fileName, self.outDir,
            self.serverIP)
        #self.sock.sendto("fin", self.serverAddr);
        return  self.outDir + self.fileName

if __name__ == "__main__":
    client = MUdpClient("/usr/lib/python2.7/dist-packages/nova/image/libmuticast_receiver.so", "10.238.152.182",
        "/var/lib/glance/images/134d2c51-c96d-4049-9821-224ac4bb07bc", "TOKEN", "134d2c51-c96d-4049-9821-224ac4bb07bc",
        "/home/openstack/tmp/")
    imagefile = client.fetchImage()
    print imagefile

