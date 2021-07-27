package com.deejay.tomcatratelimiter;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.primitives.Doubles;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TomcatRateLimiter extends ValveBase {
    String controllerUrl = "/controller";
    private double rateLimit = 1.0 ;
    private String rateAPIKey = "";
    RateLimiter rateLimiter = RateLimiter.create(rateLimit);
    Boolean rateSetFlag = false;

    public  void setRateLimit(String rate) { this.rateLimit = Double.parseDouble(rate); }

    public Double getRateLimit()  { return  rateLimit; }

    private String getRateAPIKey() {
        String apiSecret = "";
        apiSecret = System.getProperty("Rate-Controller-API-Secret");
        return apiSecret;
    }


    private boolean isValidateRateString(String strRate) {
        boolean validRate = false;
        if (Doubles.tryParse(strRate) != null) {
            validRate = true;
            containerLog.info("Valid Rate received in Controller request");
        }
        return  validRate;
    }

    public String parseRateString(String strQueryStr)  {
        String strRate = "";
        String pattern = "rate=(.*)";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(strQueryStr);
        if (m.find()) {
            strRate = m.group(1);
            containerLog.debug("Valid Query Parameter in the controller request");
        }
        return strRate;
    }

    public boolean allowedToControlRate(String apiKey) {
      String rateAPIKeyHeader = apiKey;
      Boolean allowedToControlRate = false;

      containerLog.debug("rateAPIKeyHeader=" + rateAPIKeyHeader);
      if (rateAPIKeyHeader.equals(Base64.getEncoder().encodeToString(getRateAPIKey().getBytes(StandardCharsets.UTF_8)))) {
          allowedToControlRate =  true;
          containerLog.info("API Key Validation is successful");
      }
      else
      {
          allowedToControlRate = false;
          containerLog.error("API Key Validation has failed");
      }
      return allowedToControlRate;
    }

    private Boolean IfHeaderExists(Enumeration headerlist) {
        Boolean headerFlag = false;

        while(headerlist.hasMoreElements()) {
            String key = (String) headerlist.nextElement();

            if (key.equalsIgnoreCase("Rate-API-Key")) {
                headerFlag = true;
                containerLog.debug("Header Rate-API-Key exists in the controller request");
                break;
            }
                    }
        containerLog.debug("headerFlag : " + headerFlag);
        return  headerFlag;
    }

    @Override
    public final void invoke(Request request, Response response) throws ServletException, IOException {
        long startTime = 0;
        long timeTaken = 0;

        String strControlRate = "";
        String rateAPIKeyHeader = "";
        Double newRate;

        String requestURI = request.getDecodedRequestURI();
        String requestQuery = request.getQueryString();

        containerLog.info("====== New Request received with URI : " + requestURI + " ===== ");

        if (!rateSetFlag)  {
            containerLog.info("Setting the initial Rate Limit to " + getRateLimit());
            try {
                rateLimiter.setRate(getRateLimit());
                containerLog.info("Set the Initial Rate Limit to " + rateLimiter.getRate() + " for the Valve");
                rateSetFlag = true;
            } catch (Exception e) {
                containerLog.error("Couldn't set the rate successfully");
                e.printStackTrace();
            }
        }

        if (requestURI.equals(controllerUrl)) {
            containerLog.info("Received a Rate Controller Request");
            containerLog.info("Extracting Query String : " +  requestQuery);

            Enumeration headerNames = request.getHeaderNames();

            if (IfHeaderExists(headerNames)) {
                containerLog.debug("Extracting the Value for rateAPIKey");
                rateAPIKeyHeader = request.getHeader("Rate-API-Key");
                containerLog.debug("rateAPIKey Header Value : " + rateAPIKeyHeader);
                strControlRate = parseRateString(requestQuery);

                if (isValidateRateString(strControlRate) && allowedToControlRate(rateAPIKeyHeader)) {
                    newRate = Doubles.tryParse(strControlRate);

                    containerLog.debug("Rate API Key taken from System Property : " + getRateAPIKey());
                    containerLog.info("Controlling the rate with new rate : " +  newRate);

                    try {
                        rateLimiter.setRate(newRate);
                        containerLog.info("New Rate after the requested rate set : " + rateLimiter.getRate());
                        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
                    } catch (Exception e) {
                        containerLog.error("Couldn't set the requested rate successfully");
                        e.printStackTrace();
                    }
                }
                else {
                    containerLog.info("Invalid control rate or rateAPIKey ");
                    }
                               }
            else    {
                containerLog.info("Invalid request or Invalid Rate-API-Key header in the controller request ");
            }
            return;
        }

        containerLog.info("Waiting to acquire the Token for the request");
        containerLog.info("Current Rate is " +  rateLimiter.getRate());
        containerLog.info("Toekn received in : " + rateLimiter.acquire() + " seconds") ;

        startTime = System.currentTimeMillis();
        getNext().invoke(request,response);
        timeTaken = System.currentTimeMillis() - startTime;

        containerLog.info("Request processing took " + timeTaken + " ms (" + request.getDecodedRequestURI() + ")");
        containerLog.info("Request Processing for Request URI " + request.getDecodedRequestURI() + " has been completed");

    }


}
