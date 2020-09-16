#!/bin/sh

sudo umount /dev/vdb;
sudo e2fsck /dev/vdb;
sudo resize2fs /dev/vdb;
sudo mount /dev/vdb /mnt/temp 
