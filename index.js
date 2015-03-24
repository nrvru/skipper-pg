/**
 * Module dependencies
 */

var path = require('path');
var Writable = require('stream').Writable;
var _ = require('lodash');
var pg = require('pg');
var LargeObjectManager = require('pg-large-object').LargeObjectManager;

/**
 * skipper-pg
 *
 * @param  {Object} globalOpts
 * @return {Object}
 */

module.exports = function PGStore(globalOpts){
    globalOpts = globalOpts || {};
    _.defaults(globalOpts, {
        //uri: '',
        //bufferSize: 16384
    });

    var adapter = {
        ls: function(dirname, cb){
            console.log('Method ls is used, dirname: ' + dirname);

            pg.connect(globalOpts.uri, function(err, client, done){
                if(err){
                    console.log('Unable to connect to database');
                    return cb(err);
                }

                client.query('SELECT oid FROM pg_largeobject_metadata', function(err, result) {
                    if(err){
                        console.log(err);
                        cb(err);
                        done();
                    }
                    //console.log(result);
                    return cb(null, result.rows);
                });
            });
        },
        read: function (fd, cb) {
            console.log('Method read is used');
            return cb();
        },
        rm: function(){
            console.log('Method rm is used');
            return cb();
        },
        receive: function PGReceiver(options){
            console.log('Method receive is used');

            options = options || {};
            options = _.defaults(options, globalOpts);

            //console.log(options);

            var receiver__ = Writable({
                objectMode: true
            });

            // This `_write` method is invoked each time a new file is received
            // from the Readable stream (Upstream) which is pumping filestreams
            // into this receiver.  (filename === `__newFile.filename`).
            receiver__._write = function onFile(__newFile, encoding, doneReseiver) {
                console.log(__newFile);

                pg.connect(options.uri, function(err, client, done){
                    if (err) {
                        receiver__.emit('error', err);
                    }

                    //console.log('PG connected');

                    var manager = new LargeObjectManager(client);
                    client.query('BEGIN', function(err, result) {
                        if (err) {
                            done();
                            receiver__.emit('error', err);
                            return console.error('Unable to start transaction', err);
                        }

                        //console.log('Transaction begins');

                        //todo deal with dones
                        manager.createAndWritableStream(options.bufferSize, function(err, oid, stream){
                            if (err) {
                                done();
                                receiver__.emit('error', err);
                                return console.error('Unable to create a new large object', err);
                            }
                            // todo count file size
                            var size = 0;
                            //console.log('WritableStream created');
                            //console.log('oid: ' + oid);

                            __newFile.once('error', function (err) {
                                receiver__.emit('error', err);
                                console.log('***** READ error on file ' + __newFile.filename, '::', err);
                            });

                            stream.once('error', function() {
                                console.log('Error on Writable stream');
                                client.query('COMMIT');
                                done();
                            });

                            stream.once('finish', function() {
                                //console.log('closed output stream for',__newFile.fd);
                                //console.log(__newFile);
                                var dirname = __newFile.dirname || null;
                                console.log(__newFile.fd);
                                console.log(dirname);
                                var query = 'INSERT INTO file_metadata (filename, dirname, filesize, oid) VALUES (' +
                                    __newFile.filename + ',' + dirname + ',' + 0 + ',' + oid + ')';

                                client.query(query, function(err, result){
                                    if(err){
                                        receiver__.emit('error', err);
                                        return console.log('Unable to add metadata: ', err);
                                    }
                                    client.query('COMMIT');
                                    done();
                                    doneReseiver();
                                });
                            });

                            stream.once('close', function doneWritingFile(file) {
                                //console.log('closed output stream for',__newFile.fd);
                                client.query('COMMIT');
                                done();
                            });

                            __newFile.pipe(stream);
                        });
                    })
                });
            };

            return receiver__;
        }
    };

    return adapter;
};