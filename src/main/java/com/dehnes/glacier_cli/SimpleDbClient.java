package com.dehnes.glacier_cli;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.*;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SimpleDbClient {

    private final AmazonSimpleDBClient client;
    private final String domain;

    public SimpleDbClient(String domain, Regions region) {
        this.domain = domain;
        ProfileCredentialsProvider credentials = new ProfileCredentialsProvider();

        client = new AmazonSimpleDBClient(credentials);
        client.configureRegion(region);

        ListDomainsResult result = client.listDomains();
        if (!result.getDomainNames().stream().filter(d -> Objects.equals(this.domain, d)).findFirst().isPresent()) {
            CreateDomainRequest createDomainRequest = new CreateDomainRequest()
                    .withDomainName(domain);
            client.createDomain(createDomainRequest);
            System.out.println("Created new domain in SimpleDb " + domain);
        }
    }

    public void set(String key, List<Attribute> attr) {
        PutAttributesRequest request = new PutAttributesRequest()
                .withDomainName(domain)
                .withItemName(key)
                .withAttributes(attr.stream().map(a -> new ReplaceableAttribute(a.getName(), a.getValue(), true)).collect(Collectors.toList()));
        client.putAttributes(request);
    }

    public List<Attribute> get(String key) {
        GetAttributesRequest request = new GetAttributesRequest()
                .withDomainName(domain)
                .withItemName(key);
        return client.getAttributes(request).getAttributes();
    }
}
