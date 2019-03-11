// Copyright <YEAR> Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;

import java.util.regex.Pattern;

public class Validator {

    public String validateAndTitleCaseCity(String city) {
        Pattern cityPattern = Pattern.compile("^[ A-Za-z]+$");
        if(!cityPattern.matcher(city).matches()) {
            throw  new RuntimeException("City does not match the given regex.");
        }

        // pretty formatted city name
        return WordUtils.capitalize(StringUtils.lowerCase(city));
    }
}
