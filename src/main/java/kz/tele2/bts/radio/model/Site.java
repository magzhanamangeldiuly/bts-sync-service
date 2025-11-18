package kz.tele2.bts.radio.model;

import kz.tele2.bts.radio.utils.Utils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

import static java.util.Objects.isNull;
import static kz.tele2.bts.radio.utils.Utils.coalesce;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Site {
    private String name;
    private String bsc;
    private String rnc;
    private String state;
    private String operator;
    private String source;
    private Double longitude;
    private Double latitude;
    private String address;
    private String kato;

    public Site appendAtollProp(Map<String, Object> atollProperties) {
        if (isNull(atollProperties)){
            return this;
        }
        var longitude = (Double) coalesce(this.longitude, atollProperties.get("longitude"));
        var latitude = (Double) coalesce(this.latitude, atollProperties.get("latitude"));
        var address = (String) coalesce(atollProperties.get("address"), "");
        var kato = (String) coalesce(atollProperties.get("kato"), "");
        return new Site(
                this.name,
                this.bsc,
                this.rnc,
                this.state,
                this.operator,
                this.source,
                longitude,
                latitude,
                address,
                kato
        );
    }
}

