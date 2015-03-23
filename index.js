/**
 * Module dependencies
 */

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

        },
        rm: function(){

        },
        read: function (fd, cb) {

        },
        receive: function PGReceiver(options){
            options = options || {};
            options = _.defaults(options, globalOpts);

            console.log(options.uri);

            var receiver__ = Writable({
                objectMode: true
            });

            // This `_write` method is invoked each time a new file is received
            // from the Readable stream (Upstream) which is pumping filestreams
            // into this receiver.  (filename === `__newFile.filename`).
            receiver__._write = function onFile(__newFile, encoding, doneReseiver) {
                pg.connect(options.uri, function(err, client, done){
                    if (err) {
                        receiver__.emit('error', err);
                    }

                    console.log('PG connected');

                    var manager = new LargeObjectManager(client);
                    client.query('BEGIN', function(err, result) {
                        if (err) {
                            done();
                            receiver__.emit('error', err);
                            return console.error('Unable to start transaction', err);
                        }

                        console.log('Transaction begins');

                        //todo deal with dones
                        manager.createAndWritableStream(options.bufferSize, function(err, oid, stream){
                            if (err) {
                                done();
                                receiver__.emit('error', err);
                                return console.error('Unable to create a new large object', err);
                            }

                            console.log('WritableStream created');
                            console.log('oid: ' + oid);

                            __newFile.once('error', function (err) {
                                receiver__.emit('error', err, db);
                                console.log('***** READ error on file ' + __newFile.filename, '::', err);
                            });

                            stream.once('error', function() {
                                console.log('Error on Writable stream');
                                client.query('COMMIT');
                                done();
                            });

                            stream.once('finish', function() {
                                console.log('closed output stream for',__newFile.fd);
                                client.query('COMMIT');
                                done();
                                doneReseiver();
                            });

                            stream.once('close', function doneWritingFile(file) {
                                console.log('closed output stream for',__newFile.fd);
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