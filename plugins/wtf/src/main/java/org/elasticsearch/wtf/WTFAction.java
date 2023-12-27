/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.wtf;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class WTFAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(WTFAction.class);
    private final Map<String, RestRequest> init_info = new HashMap<>();

    @Override
    public List<Route> routes() {
        // _cluster may not be the best place for the wtf endpoints,
        // but I don't want to figure out how to make a new parent endpoint
        return List.of(new Route(GET, "/_cluster/wtf/trace/{index}"), new Route(POST, "/_cluster/wtf/init/{index}"));
    }

    @Override
    public String getName() {
        return "wtf";
    }

    // Parse WTF API URL from stored init request
    private String initRequestWtfApiUrl(final RestRequest request) {
        String wtf_api_url = null;
        Map<String, Object> body = new HashMap<String, Object>();

        try {
            request.applyContentParser(parser -> body.putAll(parser.map()));
        } catch (final IOException e) {
            logger.warn("Exception parsing WTF API URL from init REST request; cannot forward trace upstream", e);
        }
        for (var kv : body.entrySet()) {
            logger.info(kv.getKey() + ":" + kv.getValue());
            if (kv.getKey() == "wtf_api_url") {
                wtf_api_url = kv.getValue().toString();
            }
        }

        if (wtf_api_url == null) {
            logger.warn("No WTF API URL found in init REST request; cannot forward trace upstream");
        }
        return wtf_api_url;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        /**
         * To build:
         * ./gradlew build -p plugins/wtf
            rm -rf distribution/archives/linux-tar/build/install/elasticsearch-8.9.0-SNAPSHOT/plugins/wtf
            /distribution/archives/linux-tar/build/install/elasticsearch-8.9.0-SNAPSHOT/bin/elasticsearch-plugin install \
                file:///home/emily/projects/wtf_project/elasticsearch/plugins/wtf/build/distributions/wtf-8.9.0-SNAPSHOT.zip
         */

        logger.error("ZZEM params: ");
        request.params().forEach((k, v) -> logger.info(k + ":" + v));

        final String index = request.param("index");

        RestResponse response = null;
        if (request.method() == POST) {
            // store upstream
            init_info.put(index, request);
            response = new RestResponse(RestStatus.OK, "Stored upstream init request");
        } else if (request.method() == GET) {
            // send a trace to upstream
            final RestRequest init_request = init_info.get(index);
            if (init_request == null) {
                logger.warn("Received trace request for index " + index + " with no upstream history; trace terminates here");
                response = new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, "Missing upstream history");
            } else {
                // TODO send trace request to prev hop (for now manually curling when this msg is logged)
                logger.info("Send trace for index " + index + " with body...");
                final String wtf_api_url = initRequestWtfApiUrl(init_request);
                if (wtf_api_url == null) {
                    response = new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, "");
                } else {
                    logger.info("...to upstream WTF API URL at " + wtf_api_url);
                    response = new RestResponse(RestStatus.OK, "Sent to upstream");
                }
            }
        }
        // TODO send trace response back to initiator
        final RestResponse response_copy = response; // quiet compiler
        return channel -> {
            try {
                channel.sendResponse(response_copy);
            } catch (final Exception e) {
                channel.sendResponse(new RestResponse(channel, e));
            }
        };
    }

    // TODO what is this
    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

}
