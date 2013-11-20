/**
 * This application ships mongodb metrics to a local zabbix trapper agent.
 * @author Robert Schmalholz <robert@teaminternet.de>
 * @author Team Internet AG
 *
 * Licensed under Apache License 2.0
 * Check LICENSE for additional information
 */

// how often should the servers be polled for data
var CHECK_INTERVAL = 60 * 1000;

// where is the zabbix trapper located
var ZABBIX_HOST = "127.0.0.1";

var fs = require("fs"), http = require("http"), crypto = require("crypto"), cp = require("child_process");

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
	} else {
		ret += host + " " + parents + " " + ts + " " + obj + "\n";
	}

	return ret;
};

/**
 * Checks replication information for given host via its web interface
 */
var checkRepl = function(host, visHost) {
	var uniqID = sha1(new Date().getTime() + "::" + Math.random());

	console.log("INFO: [" + host + "] [repl] checking replica info ...");
	http.get("http://" + host + ":28017/replSetGetStatus", function(res) {
		var rawbody = "";
		res.on("end", function() {
			try {
				var body = JSON.parse(new Buffer(rawbody, 'binary').toString()), primary = false;
				body.membersNeedAttention = 0;
				for(var member in body.members) {
					if(body.members[member].state < 1 || body.members[member].state > 2)
						body.membersNeedAttention ++;
					if(body.members[member].self == true)
						body.self = body.members[member];
					if(body.members[member].state == 1)
						primary = body.members[member];
				}

				if(primary && body.self && primary.optime && primary.optime.$timestamp.t && body.self.optime && body.self.optime.$timestamp.t) {
					body.self.slaveLag = primary.optime.$timestamp.t - body.self.optime.$timestamp.t;
					if(body.self.slaveLag < 0)
						body.self.slaveLag = 0;
				}

				delete body.members;
				var list = buildRecursiveList(visHost, "mongodb_repl", body, Math.round(new Date().getTime()/1000));

				fs.writeFile("/tmp/.mrepl-" + host, list, function(err){});

				fs.writeFile("/tmp/zabbix-sender-" + uniqID, list, function(err) {
					if(err) {
						console.log("ERR : [" + host + "] [repl] couldn't write file");
					} else {
						cp.execFile("zabbix_sender", ["-vv", "-T", "-z", ZABBIX_HOST, "-i", "/tmp/zabbix-sender-" + uniqID], {}, function(err, stdout, stderr) {
							if(re = stdout.toString().match(/sent: ([0-9]+); skipped: ([0-9]+); total: ([0-9]+)/)) {
								console.log("INFO: [" + host + "] [repl] sent " + re[1] + "/" + re[3] + " values!");
							}

							if(re = stderr.toString().replace(/[\n\r]/, "").match(/\[(\{.+\})\]/)) {
								try {
									var json = JSON.parse(re[1]);
									if(json.response == "success") {
										console.log("INFO: [" + host + "] [repl] zabbix_sender: " + json.info);
									} else {
										console.log("ERR : [" + host + "] [repl] zabbix_sender: " + json.info);
									}
								} catch(e) {
									console.log("ERR : [" + host + "] [repl] invalid json from zabbix_sender!");
								}
							}
							fs.unlink("/tmp/zabbix-sender-" + uniqID);
						});
					}
				});
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
 * Checks server status for a given host via its web interface
 */
var checkHost = function(host, visHost) {
	var uniqID = sha1(new Date().getTime() + "::" + Math.random());

	console.log("INFO: [" + host + "] checking server info ...");
	http.get("http://" + (host.match(/:/) ? host : (host + ":28017")) + "/_status", function(res) {
		var rawbody = "";
		res.on("end", function() {
			try {
				var body = JSON.parse(new Buffer(rawbody, 'binary').toString());
				var list = buildRecursiveList(visHost, "mongodb", body.serverStatus, Math.round(new Date().getTime()/1000));

				if(body.serverStatus.repl && body.serverStatus.repl.setName) setImmediate(checkRepl, host, visHost);

				fs.writeFile("/tmp/.mstatus-" + host, list, function(err){});

				fs.writeFile("/tmp/zabbix-sender-" + uniqID, list, function(err) {
					if(err) {
						console.log("ERR : [" + host + "] [glob] couldn't write file");
					} else {
						cp.execFile("zabbix_sender", ["-vv", "-T", "-z", ZABBIX_HOST, "-i", "/tmp/zabbix-sender-" + uniqID], {}, function(err, stdout, stderr) {
							if(re = stdout.toString().match(/sent: ([0-9]+); skipped: ([0-9]+); total: ([0-9]+)/)) {
								console.log("INFO: [" + host + "] [glob] sent " + re[1] + "/" + re[3] + " values!");
							}

							if(re = stderr.toString().replace(/[\n\r]/, "").match(/\[(\{.+\})\]/)) {
								try {
									var json = JSON.parse(re[1]);
									if(json.response == "success") {
										console.log("INFO: [" + host + "] [glob] zabbix_sender: " + json.info);
									} else {
										console.log("ERR : [" + host + "] [glob] zabbix_sender: " + json.info);
									}
								} catch(e) {
									console.log("ERR : [" + host + "] [glob] invalid json from zabbix_sender!");
								}
							}
							fs.unlink("/tmp/zabbix-sender-" + uniqID);
						});
					}
				});
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
 * Main loop
 */
var checkAll = function() {
	console.log("INFO: new round " + new Date());
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

checkAll();
setInterval(checkAll, CHECK_INTERVAL);
