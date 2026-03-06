import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

SimpleHttpResponse response = http.postForm(
        "https://api.example.com/login",
        Map.of(
                "username", "alice",
                "password", "secret"
        ),
        null
);

public class HttpService {

    private final CloseableHttpClient client;

    public HttpService() {
        this.client = HttpClients.custom()
                .setMaxConnTotal(100)
                .setMaxConnPerRoute(20)
                .build();
    }

    public SimpleHttpResponse get(String url,
                                  Map<String, String> queryParams,
                                  Map<String, String> headers) throws Exception {

        URIBuilder builder = new URIBuilder(url);

        if (queryParams != null) {
            for (Map.Entry<String, String> entry : queryParams.entrySet()) {
                builder.addParameter(entry.getKey(), entry.getValue());
            }
        }

        URI uri = builder.build();
        HttpGet request = new HttpGet(uri);

        if (headers != null) {
            headers.forEach(request::addHeader);
        }

        return execute(request);
    }

    public SimpleHttpResponse postJson(String url,
                                       String json,
                                       Map<String, String> headers) throws Exception {

        HttpPost request = new HttpPost(url);

        request.setEntity(new StringEntity(json, "UTF-8"));
        request.setHeader("Content-Type", "application/json");

        if (headers != null) {
            headers.forEach(request::addHeader);
        }

        return execute(request);
    }

    public SimpleHttpResponse postForm(String url,
                                       Map<String, String> params,
                                       Map<String, String> headers) throws Exception {

        HttpPost request = new HttpPost(url);

        if (params != null && !params.isEmpty()) {

            List<NameValuePair> formParams = new ArrayList<>();

            for (Map.Entry<String, String> entry : params.entrySet()) {
                formParams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }

            request.setEntity(new UrlEncodedFormEntity(formParams, "UTF-8"));
        }

        if (headers != null) {
            headers.forEach(request::addHeader);
        }

        return execute(request);
    }

    private SimpleHttpResponse execute(HttpUriRequest request) throws Exception {

        try (CloseableHttpResponse response = client.execute(request)) {

            int status = response.getStatusLine().getStatusCode();

            HttpEntity entity = response.getEntity();
            String body = entity != null
                    ? EntityUtils.toString(entity)
                    : null;

            return new SimpleHttpResponse(status, body);
        }
    }
}

public class SimpleHttpResponse {

    private final int status;
    private final String body;

    public SimpleHttpResponse(int status, String body) {
        this.status = status;
        this.body = body;
    }

    public int getStatus() {
        return status;
    }

    public String getBody() {
        return body;
    }

    public boolean isSuccess() {
        return status >= 200 && status < 300;
    }
}


