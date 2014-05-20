/**
 * This application ships mongodb metrics to a local zabbix trapper agent.
 * @author Robert Schmalholz <robert@teaminternet.de>, Markus Ostertag <markus@teaminternet.de>
 * @author Team Internet AG
 *
 * Licensed under Apache License 2.0
 * Check LICENSE for additional information
 */

// how often should the servers be polled for data
var CHECK_INTERVAL = 60 * 1000;

// where is the zabbix trapper located
var ZABBIX_HOST = "127.0.0.1";

// Don't trigger the sender just output to console
var TESTING = false;

// use a db connection or the http API
var USE_HTTP = false;

var fs = require("fs"), http = require("http"), crypto = require("crypto"), cp = require("child_process");
if (!USE_HTTP){
	var mongodb = require("mongodb");
	var MongoConn = [];
}

var sha1 = function(str) {
	return crypto.createHash('sha1').update(String(str)).digest('hex');
};

/**
 * Recursively transfers a json object to a comma separated list
 */
var buildRecursiveList = function(host, parents, obj, ts) {
	var ret = "";

	if(typeof obj == 'object') {
		if(obj.constructor.name == 'Array') {
			ret += host + " " + parents + "._length " + ts + " " + obj.length + "\n";
		}

		for(var key in obj) {
			var skey = key == '.' ? '_global' : key;
			ret += buildRecursiveList(host, (parents ? parents + "." : "") + skey, obj[key], ts);
		}
	} else if (typeof obj == 'function') {
		// just do nothing
	} else {
		ret += host + " " + parents + " " + ts + " " + obj + "\n";
	}

	return ret;
};

var checkRepl;

/**
 * Checks replication information for given host via db connection
 */
var checkReplDB = function(host, visHost) {
	console.log("INFO: [" + host + "] [repl] checking replica info ...");
	getDBConnection(host,27017,function(conn){
		if (conn){
			conn.command({"replSetGetStatus": 1}, function(err, res){
				if (err || !res){
					console.log("ERR : [" + host + "] [repl] error during db command received! "+err);
				} else {
					parseRepl(res,host,visHost);
				}
			});
		}
	});
}


/**
 * Checks replication information for given host via its web interface
 */
var checkReplHTTP = function(host, visHost) {
	console.log("INFO: [" + host + "] [repl] checking replica info ...");
	http.get("http://" + host + ":28017/replSetGetStatus", function(res) {
		var rawbody = "";
		res.on("end", function() {
			try {
				var body = JSON.parse(new Buffer(rawbody, 'binary').toString());
				parseRepl(body,host,visHost);
			} catch(e) {
				console.log("ERR : [" + host + "] [repl] invalid json received!");
			}
		});
		res.on("data", function(chunk) {
			rawbody += chunk.toString('binary');
		});
	}).on("error", function(err) {
		console.log("ERR: [" + host + "] [repl] http failure!");
	});
}

/**
 * Parses the repl data, saves it and triggers the trapper
 */
var parseRepl = function(data, host, visHost){
	var uniqID = sha1(new Date().getTime() + "::" + Math.random());
	var primary = false;
	data.membersNeedAttention = 0;
	for(var member in data.members) {
		if(data.members[member].state < 1 || data.members[member].state > 2)
			data.membersNeedAttention ++;
		if(data.members[member].self == true)
			data.self = data.members[member];
		if(data.members[member].state == 1)
			primary = data.members[member];
	}

	// due to the stupid extended json (http://docs.mongodb.org/manual/reference/mongodb-extended-json/) of mongodb we need to split here
	if (USE_HTTP) {
		if(primary && data.self && primary.optime && primary.optime.$timestamp.t && data.self.optime && data.self.optime.$timestamp.t) {
			data.self.slaveLag = primary.optime.$timestamp.t - data.self.optime.$timestamp.t;
			if(data.self.slaveLag < 0)
				data.self.slaveLag = 0;
		}
	} else {
		if(primary && data.self && primary.optime && data.self.optime) {
			data.self.slaveLag = primary.optime.subtract(data.self.optime).getHighBits();
			if(data.self.slaveLag < 0)
				data.self.slaveLag = 0;
		}
	}

	delete data.members;
	var list = buildRecursiveList(visHost, "mongodb_repl", data, Math.round(new Date().getTime()/1000));

	fs.writeFile("/tmp/.mrepl-" + host, list, function(err){});

	var file = "/tmp/zabbix-sender-" + uniqID;
	fs.writeFile(file, list, function(err) {
		if(err) {
			console.log("ERR : [" + host + "] [repl] couldn't write file");
		} else {
			triggerTrapper(file,host, function(){
				fs.unlink(file);
			})
		}
	});
}


var checkHost;

/**
 * Checks server status for a given host via its web interface
 */
var checkHostDB = function(host, visHost) {
	console.log("INFO: [" + host + "] checking server info ...");
	getDBConnection(host,27017,function(conn){
		if (conn){
			conn.command({"serverStatus": 1}, function(err, res){
				if (err || !res){
					console.log("ERR : [" + host + "] [glob] error during db command received! "+err);
				} else {
					parseHost(res,host,visHost);
				}
			});
		}
	});
}

/**
 * Checks server status for a given host via its web interface
 */
var checkHostHTTP = function(host, visHost) {
	console.log("INFO: [" + host + "] checking server info ...");
	http.get("http://" + (host.match(/:/) ? host : (host + ":28017")) + "/_status", function(res) {
		var rawbody = "";
		res.on("end", function() {
			try {
				var body = JSON.parse(new Buffer(rawbody, 'binary').toString());
				parseHost(body.serverStatus,host,visHost);

			} catch(e) {
				console.log("ERR : [" + host + "] [glob] invalid json received!");
			}
		});
		res.on("data", function(chunk) {
			rawbody += chunk.toString('binary');
		});
	}).on("error", function(err) {
		console.log("ERR: [" + host + "] [glob] http failure!");
	});
}

/**
 *  Parses the serverStatus, saves it and triggers the trapper
 */
var parseHost = function(data,host,visHost){
	var uniqID = sha1(new Date().getTime() + "::" + Math.random());
	var list = buildRecursiveList(visHost, "mongodb", data, Math.round(new Date().getTime()/1000));

	if(data.repl && data.repl.setName){
		setImmediate(checkRepl, host, visHost);
	}

	fs.writeFile("/tmp/.mstatus-" + host, list, function(err){});
	var file = "/tmp/zabbix-sender-" + uniqID;
	fs.writeFile(file, list, function(err) {
		if(err) {
			console.log("ERR : [" + host + "] [glob] couldn't write file");
		} else {
			triggerTrapper(file, host, function(){
				fs.unlink(file);
			});
		}
	});

}


/**
 * Executes the local zabbix_sender with the given file as datasource
 */
var triggerTrapper = function(dataFile, host, callback){
	if (TESTING){
		fs.readFile(dataFile, {encoding:"utf8"},function (err, data) {
			if (err) throw err;
			console.log(data);
			callback();
		});
	}
	else {
		cp.execFile("zabbix_sender", ["-vv", "-T", "-z", ZABBIX_HOST, "-i", dataFile], {}, function(err, stdout, stderr) {
			if(re = stdout.toString().match(/sent: ([0-9]+); skipped: ([0-9]+); total: ([0-9]+)/)) {
				console.log("INFO: [" + host + "] sent " + re[1] + "/" + re[3] + " values!");
			}

			if(re = stderr.toString().replace(/[\n\r]/, "").match(/\[(\{.+\})\]/)) {
				try {
					var json = JSON.parse(re[1]);
					if(json.response == "success") {
						console.log("INFO: [" + host + "] zabbix_sender: " + json.info);
					} else {
						console.log("ERR : [" + host + "] zabbix_sender: " + json.info);
					}
				} catch(e) {
					console.log("ERR : [" + host + "] invalid json from zabbix_sender!");
				}
			}
			callback();
		});
	}
}

/**
 * We want to reuse our connections and keep them open... this function helps us to identify already opened connections
 */
var getDBConnection = function(host, port, callback){
	port = port || 27017;
	var identifier = host.toString()+port.toString();
	if (!MongoConn[identifier]){
		MongoConn[identifier] = new mongodb.Db("admin", new mongodb.Server(host, port,{auto_reconnect:true}), {safe: true, w:0});
		MongoConn[identifier].open(function(err) {
			if (err){
				console.log("ERR: [" + host + "] [glob] db connection failure!");
				MongoConn[identifier] = null;
				callback(null);
			}
			else {
				callback(MongoConn[identifier]);
			}
		});
	} else {
		callback(MongoConn[identifier]);
	}
}

/**
 * Main loop
 */
var checkAll = function() {
	if (USE_HTTP){
		checkHost = checkHostHTTP;
		checkRepl = checkReplHTTP;
	} else {
		checkHost = checkHostDB;
		checkRepl = checkReplDB;
	}
	console.log("INFO: new round " + new Date());
	if (!fs.existsSync("hosts.txt")){
		console.log("INFO : hosts list doesn't exist! I fall back to localhost only");
		checkHost("localhost","localhost");
	} else {
		fs.readFile("hosts.txt", function(err, data) {
			if(err || !data) {
				console.log("ERR : couldn't read hosts list!");
			} else {
				var hosts = data.toString().split(/\n/);
				for(var i=0; i<hosts.length; i++) {
					var host = hosts[i].trim();
					if(host) {
						var hostParts = host.split(/,/);
						if(hostParts.length == 1) {
							checkHost(host, host);
						} else {
							checkHost(hostParts[0], hostParts[1]);
						}
					}
				}
			}
		});
	}
}

checkAll();
setInterval(checkAll, CHECK_INTERVAL);
