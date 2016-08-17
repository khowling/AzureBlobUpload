const fs = require('fs');
const https = require('https');
// application/octet-stream

const storageaccurl = "testupload01.blob.core.windows.net",
      acccontainer = "files",
      sas = "sv=2015-04-05&" + 
            "ss=b&" +
            "srt=sco&" +
            "sp=rwdlac&" +
            "se=2017-08-15T19:03:04Z&" +
            "st=2016-08-15T11:03:04Z&" +
            "sip=0.0.0.0-255.255.255.255&" +
            "spr=https&" +
            "sig=SXSxvatYtHRjR1ga2LtymPqQC530LSwl4q271NB9CAE%3D"

// ---------------------------------------------- DELETE BLOCKS
function deleteblobs (fileName) {
    console.log (`/${acccontainer}/${encodeURIComponent(fileName)}?comp=blocklist&${sas}`)
    let getreq = https.request({
        hostname: storageaccurl,
        path: `/${acccontainer}/${encodeURIComponent(fileName)}?comp=blocklist&blocklisttype=uncommitted&${sas}`,
        method: 'GET'
        }, (res) => {
            res.on('data', (d) => {
                console.error (`on data ${d}`)

                let rows = d.split("<Name>").slice(1),
                    blockids = rows.map ((i) => i.substr(0,i.indexOf("</Name>")))
                

            });

            console.log (res.statusCode)
        }).on('error', (e) =>   console.log(e));
    getreq.end()
}

// ---------------------------------------------- creates a new block blob
function putblock (fileName, blockid, data) {
    return new Promise ((acc,rej) => {
        //comp=blocklist
        let comp
        if (!Array.isArray(blockid)) {
            comp = `comp=block&blockid=${new Buffer(blockid).toString('base64')}`
        } else {
            comp = "comp=blocklist"
            data = '<?xml version="1.0" encoding="utf-8"?>' +
                    '<BlockList>' +
                    blockid.map((l) => `<Latest>${new Buffer(l).toString('base64')}</Latest>`).join('') +
                    '</BlockList>'
            console.log ('putting ' + data)
        }

        console.log (`putting blockid ${blockid}, size: ${data.length.toLocaleString()} bytes (total send ${sent.toLocaleString()} settled ${settled.toLocaleString()})`)
        let putreq = https.request({
                hostname: storageaccurl,
                path: `/${acccontainer}/${encodeURIComponent(fileName)}?${comp}&${sas}`,
                method: 'PUT',
                headers: {
                    "Content-Length": data.length
                }
                }, (res) => {
                    res.on('data', (d) => {
                        console.error (`on data ${d}`)
                    });

                    if(res.statusCode == 200 || res.statusCode == 201) {
                        settled+= data.length
                        acc(blockid)
                    } else {
                        errors++
                        rej(res.statusCode)
                    }
                }).on('error', (e) =>  rej(e));

        putreq.write (data)
        putreq.end()
        sent+= data.length
    })
}

const BLOCK_SIZE = 4* 1024 * 1024,
      OUTSTANDING_BLOCKS = 10
var sent = 0, settled = 0, errors = 0, outstanding = 0

// Initially, the stream is in a static state. As soon as you listen to data event and attach a callback it starts flowing
function upload(fileName) {
    let fstat = fs.statSync (fileName),
        fsize = fstat.size, started = false,
        startt = new Date().getTime()

    let currblock = 0, sendblockids = [], allPromise = [];
    console.log (`uploading file ${fileName}, size: ${fstat.size.toLocaleString()}, blocksz: ${fstat.blksize}`)
    let rstream = fs.createReadStream(fileName, { highWaterMark: BLOCK_SIZE })

    //rstream.setEncoding(null) //  useful when working with binary data *DOESNT WORK*
    rstream.on('data', (chunk) => {

        // flow control
        outstanding++
        if (outstanding >= OUTSTANDING_BLOCKS) {
             rstream.pause()
        }

        let blockid = "KH01" + ('00000'+currblock++).slice(-5)
        sendblockids.push(blockid);
        allPromise.push (putblock(fileName, blockid, chunk).then (() => {
            outstanding--
            rstream.resume();
        }, (err) => {
            console.error (`putblock error : ${err}`)
        })) 
    }).on('end', function () {
        console.log('waiting for all to putblocks to complete');
        Promise.all (allPromise).then ((succ) => {
            console.log ('complete: bytes (total send ${sent.toLocaleString()} settled ${settled.toLocaleString()})`')
            putblock(fileName, sendblockids, null).then ((succ) => {
                console.log (`finished  ${(new Date().getTime() - startt)/1000}s`);
            });
        })
    }).on('error', (e) => console.error ('error : ' + e));
}

upload(process.argv[2])
//deleteblobs(process.argv[2])
