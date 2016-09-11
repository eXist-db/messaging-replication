package org.exist.jms.replication.subscribe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.EXistException;
import org.exist.jms.replication.shared.MessageHelper;
import org.exist.security.Account;
import org.exist.security.Group;
import org.exist.security.PermissionDeniedException;
import org.exist.security.internal.aider.GroupAider;
import org.exist.security.internal.aider.UserAider;

import java.util.Map;
import java.util.Optional;

/**
 * Created by wessels on 11/9/16.
 */
public class UserGroupHelper {

    private final static Logger LOG = LogManager.getLogger();
    String groupName;
    String userName;

    void process(final org.exist.security.SecurityManager securityManager, final Map<String, Object> metaData) {

        String inputUserName = null;
        Object prop = metaData.get(MessageHelper.EXIST_RESOURCE_OWNER);
        if (prop != null && prop instanceof String) {
            inputUserName = (String) prop;
        } else {
            LOG.debug("No username provided");
            return;
        }


        String inputGroupName = null;
        prop = metaData.get(MessageHelper.EXIST_RESOURCE_GROUP);
        if (prop != null && prop instanceof String) {
            inputGroupName = (String) prop;
        } else {
            LOG.debug("No groupname provided");
            return;
        }

        // check group first
        Group group = securityManager.getGroup(inputGroupName);
        if (group == null) {
            LOG.info(String.format("Group %s does not exist. Will be created.", inputGroupName));

            try {
                Group newGroup = new GroupAider(inputGroupName);
                securityManager.addGroup(newGroup);
                group = newGroup;
            } catch (PermissionDeniedException | EXistException e) {
                LOG.error(String.format("Unable to create group %s. Fall back to default. %s", inputGroupName, e.getMessage()));
            }

            // Fallback
            if (group == null) {
                group = securityManager.getSystemSubject().getDefaultGroup();
            }

            groupName = group.getName();
        }


        // then handle user
        Account account = securityManager.getAccount(inputUserName);
        if (account == null) {
            LOG.error(String.format("Username %s does not exist.", inputUserName));

            final Account user = new UserAider(inputUserName, group);
            try {
                securityManager.addAccount(user);
                account = user;
            } catch (PermissionDeniedException | EXistException e) {
                LOG.error(String.format("Unable to create user %s. Fall back to default. %s", inputUserName, e.getMessage()));
            }

        }

        // Fallback
        if (account == null) {
            account = securityManager.getSystemSubject();
        }

        userName = account.getName();


    }

    Optional<String> getGroupName() {
        return Optional.ofNullable(groupName);
    }

    Optional<String> getUserName() {
        return Optional.ofNullable(userName);
    }
}
