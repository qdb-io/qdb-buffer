qdb-buffer
==========

Disk based message queue supporting sequential retrieval of old messages by id and timestamp. Designed to be embedded
in JVM applications. Much more efficient than storing messages in a relational database or in a MongoDB capped
collection or whatever.

Usage
-----

Creating a new buffer:

    MessageBuffer mb = new PersistentMessageBuffer("buffer-dir");
    mb.setMaxLength(10 * 1000 * 1000000L /*10G*/ );

The buffer will store its data in files in buffer-dir, each approximately 10M in size (10G / 1000). When the buffer
is full the oldest file(s) are deleted to make space.

Appending a message:

    byte[] message = ...
    long id = mb.append(System.currentTimeMillis(), "some:routing:information", message);
    System.out.println("Appended message id " + id);
    // message id's always get bigger but are not sequential

Read messages:

    MessageCursor c = mb.cursor(0);
    while (c.next()) {
        System.out.println("id " + c.getId() + " timestamp " + c.getTimestamp() +
            " routing key [" + c.getRoutingKey() + "] payload size " + c.getPayloadSize());
        Byte[] payload = c.getPayload();
        ...
    }

You can call next() after it returns false to poll for new messages.

Read messages by timestamp:

    long timestamp = new SimpleDateFormat("yyyyMMdd HH:mm").parse("20121113 05:47").getTime();
    MessageCursor c = mb.cursorByTimestamp(timestamp);
    while (c.next()) {
        // process the message
    }

Get a timeline of messages in the buffer. This can be used to create a user interface to read old messages from a
given time onwards and so on. There will be one entry on the timeline for each segment (file) in the buffer.

    Timeline t = mb.getTimeline();
    for (int i = 0, n = t.size(); i < n; i++) {
        System.out.println("id " + t.getMessageId(i) + " timestamp " + t.getTimestamp(i) + " bytes " + getBytes(i) +
            " millis " + t.getMillis(i) + " count " + t.getCount(i));
    }

Get a more detailed timeline for the segment (file) containing the message. This is used to "drill down" and provide
more detail for part of the bigger timeline.

    Timeline t = mb.getTimeline(messageId);


Features
--------

- Efficient reading of messages from any point in time or id
- Buffer provides a timeline or histogram of messages over time (e.g. for creating a chart)
- Automatic recovery after system crash. Some messages may be lost but the buffer will not be corrupt
- Old messages are efficiently deleted when the buffer is full

License
-------

Copyright 2012 David Tinker

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
