package com.zendesk.maxwell.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huaweicloud.dis.core.DISCredentials;
import com.huaweicloud.dis.core.util.StringUtils;
import com.huaweicloud.dis.util.config.ICredentialsProvider;

public class WCCProvider implements ICredentialsProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(WCCProvider.class);
    
    private boolean decrypted = false;
    
    private DISCredentials decryptedCredentials;
    
    @Override
    public DISCredentials updateCredentials(DISCredentials disCredentials)
    {
        if (!decrypted)
        {
            synchronized (WCCProvider.class)
            {
                if (!decrypted)
                {
                    String decryptAK = WCCTool.getInstance().decrypt(disCredentials.getAccessKeyId());
                    String decryptSK = WCCTool.getInstance().decrypt(disCredentials.getSecretKey());
                    
                    String securityToken = disCredentials.getSecurityToken();
                    if (!StringUtils.isNullOrEmpty(securityToken))
                    {
                        securityToken = WCCTool.getInstance().decrypt(securityToken);
                    }
                    
                    String dataPassword = disCredentials.getDataPassword();
                    if (!StringUtils.isNullOrEmpty(dataPassword))
                    {
                        dataPassword = WCCTool.getInstance().decrypt(dataPassword);
                    }
                    
                    decryptedCredentials = new DISCredentials(decryptAK, decryptSK, securityToken, dataPassword);
                    decrypted = true;
                    LOG.info("WCCProvider decrypted successfully.");
                    return decryptedCredentials;
                }
                else
                {
                    return decryptedCredentials;
                }
            }
        }
        return disCredentials;
    }
}
