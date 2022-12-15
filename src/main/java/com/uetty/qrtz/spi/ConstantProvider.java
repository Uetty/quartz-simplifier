package com.uetty.qrtz.spi;

/**
 * 常量SPI provider
 * @author vince
 */
public interface ConstantProvider {

    String getConstantValue(String key);
}
