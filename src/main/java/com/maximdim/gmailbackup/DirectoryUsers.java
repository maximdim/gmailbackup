package com.maximdim.gmailbackup;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.api.client.auth.oauth2.TokenErrorResponse;
import com.google.api.client.auth.oauth2.TokenResponseException;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson.JacksonFactory;

/**
 * Lists the domain's mailboxes from the Admin SDK Directory API, so the set of users to back up
 * does not have to be kept up to date by hand in the properties file.
 *
 * <p>Uses the same service account and P12 key as the mail fetch. Two things are needed on the
 * Google side:
 * <ul>
 *   <li>the scope {@value #SCOPE} added to the service account's entry under
 *       Admin console &gt; Security &gt; Access and data control &gt; API controls &gt;
 *       Domain-wide delegation (alongside the existing https://mail.google.com/);</li>
 *   <li>a super administrator to impersonate - the Directory API has no notion of a service
 *       account of its own, the call is made as a real admin through domain wide delegation.</li>
 * </ul>
 * A scope added to a delegation entry takes a few minutes to propagate; until it has, the call
 * comes back as 401 unauthorized_client.
 *
 * <p>The raw REST call is used rather than google-api-services-admin-directory: the HTTP and JSON
 * machinery is already on the classpath for OAuth2, and one paged GET is not worth another
 * dependency in the shaded jar.
 */
class DirectoryUsers {
  static final String SCOPE = "https://www.googleapis.com/auth/admin.directory.user.readonly";
  private static final String USERS_URL = "https://admin.googleapis.com/admin/directory/v1/users";
  private static final int PAGE_SIZE = 200; // the API allows up to 500
  private static final int MAX_PAGES = 100; // a paging bug must not spin forever
  // the backup runs from cron every 5 minutes - a hung listing must not outlive the run
  private static final int TIMEOUT_MS = 30000;

  /**
   * @return the local part of every active mailbox in the domain, in the API's order (by email).
   */
  static List<String> list(File pkFile, String serviceAccountId, String adminUser, String domain)
      throws Exception {
    HttpTransport transport = new NetHttpTransport();
    JsonFactory jsonFactory = new JacksonFactory();
    GoogleCredential credential = new GoogleCredential.Builder()
        .setTransport(transport)
        .setJsonFactory(jsonFactory)
        .setServiceAccountId(serviceAccountId)
        .setServiceAccountUser(adminUser)
        .setServiceAccountScopes(Collections.singleton(SCOPE))
        .setServiceAccountPrivateKeyFromP12File(pkFile)
        .build();

    // refreshed up front so an impersonation that is refused is reported as itself, rather than
    // as a failure of whichever request happened to trigger the lazy refresh
    try {
      credential.refreshToken();
    }
    catch (TokenResponseException e) {
      TokenErrorResponse details = e.getDetails();
      throw new IllegalStateException(explainTokenError(
          details == null ? null : details.getError(),
          details == null ? e.getStatusMessage() : details.getErrorDescription(),
          adminUser), e);
    }

    HttpRequestFactory requests = transport.createRequestFactory(credential);
    Set<String> result = new LinkedHashSet<>();
    String pageToken = null;
    int pages = 0;
    do {
      GenericUrl url = new GenericUrl(USERS_URL);
      url.put("domain", domain);
      url.put("maxResults", PAGE_SIZE);
      url.put("projection", "basic");
      url.put("orderBy", "email");
      if (pageToken != null) {
        url.put("pageToken", pageToken);
      }
      HttpRequest request = requests.buildGetRequest(url);
      request.setParser(new JsonObjectParser(jsonFactory));
      request.setConnectTimeout(TIMEOUT_MS);
      request.setReadTimeout(TIMEOUT_MS);

      GenericJson page = request.execute().parseAs(GenericJson.class);
      result.addAll(localParts(page.get("users"), domain));
      Object next = page.get("nextPageToken");
      pageToken = next == null ? null : next.toString();
    }
    while (pageToken != null && ++pages < MAX_PAGES);

    return new ArrayList<>(result);
  }

  /**
   * A refused impersonation comes back as a bare "401 Unauthorized" unless the response body is
   * read. The OAuth error code in it is the whole diagnosis - a scope that has not propagated and
   * an adminUser outside the Workspace account look identical from the status line alone.
   */
  static String explainTokenError(String error, String description, String adminUser) {
    String hint;
    if ("unauthorized_client".equals(error)) {
      hint = "the service account's domain-wide delegation entry does not carry " + SCOPE
          + " yet - a scope added in the Admin console takes a few minutes to take effect";
    }
    else if ("invalid_grant".equals(error)) {
      hint = adminUser + " is not a user of the Workspace account this service account belongs to";
    }
    else if ("access_denied".equals(error)) {
      hint = adminUser + " may not use " + SCOPE + " - it has to be a super administrator";
    }
    else {
      hint = "impersonating " + adminUser + " was refused";
    }
    return "Directory API token request failed [" + error
        + (description == null ? "" : ": " + description) + "]: " + hint;
  }

  /**
   * The "users" member of a listing response reduced to the user names the backup works with.
   * Suspended and archived mailboxes are dropped here rather than through the API's query
   * parameter: that switches the call to the search backend, which lags behind recent changes.
   */
  static List<String> localParts(Object usersNode, String domain) {
    List<String> result = new ArrayList<>();
    if (!(usersNode instanceof List)) {
      return result; // a domain with no matching users comes back without the member at all
    }
    String suffix = "@" + domain.toLowerCase();
    for (Object o : (List<?>) usersNode) {
      if (!(o instanceof Map)) {
        continue;
      }
      Map<?, ?> user = (Map<?, ?>) o;
      if (isTrue(user.get("suspended")) || isTrue(user.get("archived"))) {
        continue;
      }
      Object email = user.get("primaryEmail");
      if (email == null) {
        continue;
      }
      String s = email.toString().trim().toLowerCase();
      if (!s.endsWith(suffix) || s.length() == suffix.length()) {
        continue; // another domain of the same account - that is a separate backup run
      }
      result.add(s.substring(0, s.length() - suffix.length()));
    }
    return result;
  }

  private static boolean isTrue(Object o) {
    return Boolean.TRUE.equals(o) || "true".equalsIgnoreCase(String.valueOf(o));
  }
}
