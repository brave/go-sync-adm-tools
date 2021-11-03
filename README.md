## Brave Sync Administration Tools

This repository contains a set of Brave Sync Administration tools for maintaining sync data in DynamoDB (currently).

### Installation

To install the tools, check out the repo, and run the following:

```
   $ make all
```

### List of Tools

`main`:  This tool sets an hour TTL on DynamoDB data that needs to be deleted.

##### Usage

In order to use `main`, the user needs to have permissions to the DynamoDB in the given Sync account.  Typically this is ran by a member of the DevOps team.
Using the `devops-admin-rw-role`, a DevOps team member can set the TTL for a given client ID by running the following:
```
   $ export AWS_ENDPOINT=https://dynamodb.us-west-2.amazonaws.com
   $ aws-vault exec sync-prod-devops-rw -- ./main delete 8XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX2
     Deleting user data for clientID 8XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX2
     Successfully set ttl for 1741 records
```
