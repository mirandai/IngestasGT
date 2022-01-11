package net.crunchdroid.service;

import net.crunchdroid.model.SeparatorField;

import java.util.List;

public interface SeparatorFieldService {

    List<SeparatorField> findAll();

    SeparatorField findOne(Long id);

    SeparatorField save(SeparatorField separatorField);

    void delete(Long id);

}
