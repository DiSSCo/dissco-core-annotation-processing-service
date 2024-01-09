package eu.dissco.annotationprocessingservice.domain;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public record MasInput(String targetField, String inputField) {

}
