package net.crunchdroid.util;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
public class ResultResponse {

    public ResultResponse(Boolean result, Object object) {
        this.result = result;
        this.object = object;
    }

    public ResultResponse(Boolean result, Object object, String messageError) {
        this.result = result;
        this.object = object;
        this.messageError = messageError;
    }

    private Boolean result;
    private Object object;
    private String messageError;
}
