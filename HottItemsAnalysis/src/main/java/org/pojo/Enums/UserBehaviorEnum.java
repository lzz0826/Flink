package org.pojo.Enums;

import org.apache.commons.lang3.StringUtils;

public enum UserBehaviorEnum {


    PV("pv","瀏覽"),
    BUY("buy","購買"),
    CART("cart","購物車"),
    FAV("fav","喜好");

    public final String value;
    public final String name;

    UserBehaviorEnum(String value,String name) {
        this.name = name;
        this.value = value;
    }

    public static UserBehaviorEnum getByValue(String value) {
        if(!StringUtils.isBlank(value)){
            for(UserBehaviorEnum info : values()){
                if(info.value.equals(value)){
                    return info;
                }
            }
        }
        return null;
    }


    public static UserBehaviorEnum parse(String name) {
        if(!StringUtils.isBlank(name)){
            for(UserBehaviorEnum info : values()){
                if(info.name.equals(name)){
                    return info;
                }
            }
        }
        return null;
    }

}
