package kz.tele2.bts.radio.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RolloutSite {
    private String name;
    private Double latitude;
    private Double longitude;
}
