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

from nova.image.client import MUdpClient
from nova.image.glance import GlanceImageService

from nova import flags
from nova.openstack.common import cfg
from nova import log as logging

LOG = logging.getLogger(__name__)

fastvm_opts = [
    cfg.BoolOpt("use_fastvm",
        default=False,
        help="Whether use fastvm to do image transfer"),
    cfg.StrOpt('libdir',
        default='/usr/lib/python2.7/dist-packages/nova/image/libmuticast_receiver.so',
        help='The lib path for the libmuticast_receiver'),
    cfg.StrOpt('fastvm_server_ip',
        default='',
        help='The fastvm server ip address'),
    cfg.StrOpt('glance_filesystem_store_datadir',
        default='/var/lib/glance/images/',
        help='The file system data store data directory path for glance in the fastvm server'),
    cfg.StrOpt('img_tmp_dir',
        default='/home/openstack/tmp/',
        help='the temporary image path that used to store the vm image'),
]

FLAGS = flags.FLAGS
FLAGS.register_opts(fastvm_opts)

CHUNKSIZE = 65536

class ImageBodyIterator(object):
    """
    A class that acts as an iterator over an image file's
    chunks of data.  This is returned as part of the result
    tuple from `glance.client.Client.get_image`
    """

    def __init__(self, source):
        """
        Constructs the object from a readable image source
        (such as an HTTPResponse or file-like object)
        """
        self.source = source

    def __iter__(self):
        """
        Exposes an iterator over the chunks of data in the
        image file.
        """
        while True:
            chunk = self.source.read(CHUNKSIZE)
            if chunk:
                yield chunk
            else:
                break


class FastVMGlanceImageService(GlanceImageService):
    """
    The image service use fast vm glance image service to do vm image file transfer
    """

    def get(self, context, image_id, data):
        """Calls out to Glance for metadata and data and writes data."""
        LOG.info("configuration info:use_fastvm: '%(use_fastvm)s' and fastvm_server_ip:'%(fastvm_server_ip)s'"
                 % {'use_fastvm': FLAGS.use_fastvm, 'fastvm_server_ip': FLAGS.fastvm_server_ip})

        if FLAGS.use_fastvm:
            LOG.info("using fastvm to do image transfering")
            LOG.info("token is '%(token)s',imageid:'%(imgid)s'" % {"token": context.auth_token, "imgid": image_id})
            client = MUdpClient(FLAGS.libdir, FLAGS.fastvm_server_ip, FLAGS.glance_filesystem_store_datadir + image_id,
                context.auth_token, str(image_id),
                FLAGS.img_tmp_dir)
            imagefile = client.fetchImage()
            LOG.info(imagefile)
            filereader = open(imagefile)
            image_chunks = ImageBodyIterator(filereader)
            for chunk in image_chunks:
                data.write(chunk)

            return self.show(context, image_id)
        else:
            return super(FastVMGlanceImageService, self).get(context, image_id, data)

