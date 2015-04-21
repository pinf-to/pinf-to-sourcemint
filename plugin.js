
exports.for = function (API) {

	var exports = {};

	exports.resolve = function (resolver, config, previousResolvedConfig) {

		var sourceHash = function () {
			if (sourceHash.__cache) return API.Q.resolve(sourceHash.__cache);
			return API.Q.nbind(API.getFileTreeHashFor, API)(API.PATH.dirname(API.getRootPath())).then(function (hash) {
				return (sourceHash.__cache = hash);
			});
		}

		return resolver({
			sourceHash: sourceHash
		}).then(function (resolvedConfig) {

			var ensureSourceHash = function (stream) {
				return API.Q.fcall(function () {
					// TODO: Allow for different hash subsets depending on stream configuration
					//       instead of using same hash for all streams!
					if (ensureSourceHash.__cache) return API.Q.resolve(ensureSourceHash.__cache);
					if (stream.sourceHash) {
						return (ensureSourceHash.__cache = stream.sourceHash);
					}
					return sourceHash().then(function (sourceHash) {
						return (ensureSourceHash.__cache = sourceHash);
					});
				});
			}

			function processStream (streamId) {

				API.console.verbose("Process stream '" + streamId + "'");
				var stream = resolvedConfig.streams[streamId];

				return ensureSourceHash(stream).then(function (sourceHash) {

					stream.sourceProgramPath = API.getRootPath();
					stream.sourceProgramHash = sourceHash;
					stream.bootPackagePath = API.programDescriptor.getBootPackagePath();
				});
			}

			return API.Q.all(Object.keys(resolvedConfig.streams).map(processStream)).then(function () {

				resolvedConfig.path = API.getTargetPath();
				resolvedConfig.dirpath = API.PATH.dirname(API.getTargetPath());

				return resolvedConfig;
			});
		});
	}

	exports.turn = function (resolvedConfig) {

		function exportToZip (options) {
			var sourcePath = API.PATH.dirname(API.getRootPath());
			var archivePath = API.PATH.join(API.getTargetPath(), options.id, options.revision, options.aspect);

			return API.Q.denodeify(function (callback) {
				return API.FS.exists(archivePath, function (exists) {
					if (exists) {
						return callback(null);
					}

					API.console.verbose("Creating archive '" + archivePath + "' from '" + sourcePath + "'");

					function ensureDirectories (callback) {
						return API.FS.exists(API.PATH.dirname(archivePath), function (exists) {
							if (exists) return callback();
							return API.FS.mkdirs(API.PATH.dirname(archivePath), callback);
						});
					}

					return ensureDirectories(function (err) {
						if (err) return callback(err);

						var command = null;
						if (options.sourceFiles) {
							command = 'echo -e "' + Object.keys(options.sourceFiles).map(function (path) {
								return path.substring(1);
							}).join("\n") + '" | tar --dereference -zcf "' + API.PATH.basename(archivePath) + '" -C "' + sourcePath + '/" -T -';
						} else {
							command = 'tar --dereference -zcf "' + API.PATH.basename(archivePath) + '" -C "' + API.PATH.dirname(sourcePath) + '/" "' + API.PATH.basename(sourcePath) + '"';
						}

		                return API.runCommands([
		                	command
		                ], {
		                	cwd: API.PATH.dirname(archivePath)
		                }, callback);
					});
				});
			})().then(function () {
				return archivePath;
			});
		}

		var catalog = {
			"uid": resolvedConfig.catalog.uid,
			"revision": resolvedConfig.catalog.revision,
			"name": resolvedConfig.catalog.name,
			"packages": {}
		};

		function processStream (streamId) {
			API.console.verbose("Process stream '" + streamId + "'");
			var stream = resolvedConfig.streams[streamId];

			function processAspect (aspectId) {
				API.console.verbose("Process aspect '" + aspectId + "' for stream '" + streamId + "'");
				var aspect = stream.aspects[aspectId];

				if (aspect.bundler === "tar.gz") {
					return exportToZip({
						id: stream.id,
						revision: stream.revision,
						aspect: aspectId,
						sourceFiles: stream.sourceFiles || null
					}).then(function (archivePath) {
						if (!catalog.packages[resolvedConfig.catalog.alias]) {
							catalog.packages[resolvedConfig.catalog.alias] = {
								"uid": API.CRYPTO.createHash("sha1").update(catalog.uid + ":" + stream.id).digest("hex"),
					            "revision": stream.revision,
					            "sourceHash": "sha1:" + stream.sourceProgramHash,
					            "aspects": {}
							};
						}
						var info = catalog.packages[resolvedConfig.catalog.alias];

						return API.Q.nbind(API.runCommands, API)([
							'openssl sha1 "' + archivePath + '"'
						]).then(function (stdout) {
							info.aspects[aspectId] = {
			                	"location": "./" + API.PATH.relative(API.getTargetPath(), archivePath),
			                	"locationHash": "sha1:" + stdout.match(/=\s*([a-z0-9]+)[\s\n]$/)[1]
			                };
						});
					});

				} else {
					throw new Error("Bundler '" + aspect.bundler + "' not supported!");
				}
			}

			return API.Q.all(Object.keys(stream.aspects).map(processAspect));
		}

		return API.Q.all(Object.keys(resolvedConfig.streams).map(processStream)).then(function () {

			var catalogPath = API.PATH.join(API.getTargetPath(), "catalog.json");
			API.console.verbose("Writing catalog to:", catalogPath);
			return API.Q.denodeify(API.FS.outputFile)(catalogPath, JSON.stringify(catalog, null, 4), "utf8");
		});
	}

	exports.spin = function (resolvedConfig) {
	}

	return exports;
}
