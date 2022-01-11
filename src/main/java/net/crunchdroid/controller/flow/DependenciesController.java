package net.crunchdroid.controller.flow;

import net.crunchdroid.model.Flow;
import net.crunchdroid.model.Wf_bundles;
import net.crunchdroid.oozie.Monitor;
import net.crunchdroid.oozie.OozieClientConfig;
import net.crunchdroid.pojo.ControlDependencies;
import net.crunchdroid.service.BundleService;
;
import net.crunchdroid.service.FlowService;
import net.crunchdroid.service.WfFlujosDepeService;
import net.crunchdroid.util.BundleValidation;
import net.crunchdroid.util.ResultResponse;
import net.crunchdroid.util.Utils;
import org.apache.oozie.client.OozieClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/dependencies")
public class DependenciesController {


    Logger logger = LoggerFactory.getLogger(FlowController.class);

    @Autowired
    private FlowService flowService;

    @Autowired
    private BundleService bundleService;

    @Autowired
    private WfFlujosDepeService wfFlujosDepeService;

    private BundleValidation validation = new BundleValidation();
    private Utils utils = new Utils();
    OozieClientConfig oozieClientConfig = new OozieClientConfig();



    @GetMapping("/list")
    public String list(Model model) {
        Wf_bundles bundles = new Wf_bundles();
        model.addAttribute("dependenciesList", bundleService.findAll());
        return "flow/list-flow-dependencies";
    }


    @GetMapping("/create")
    public String add_dependencies(Model model) {
        List<Flow> flows = flowService.findAll();
        Map<Long, String> name_flows = new HashMap<>();
        for (Flow flow : flows) {
            name_flows.put(flow.getId(), flow.getName());
        }

        model.addAttribute("fill_select", name_flows);
        model.addAttribute("bundles", new Wf_bundles());
        return "flow/flow-dependencies";

    }

    @PostMapping("save_dependecies")
    public String save_dependencies(Wf_bundles bundles, Model model) {

        List<Flow> flows = flowService.findAll();
        Map<Long, String> name_flows = new HashMap<>();
        for (Flow flow : flows) {
            name_flows.put(flow.getId(), flow.getName());
        }

        ResultResponse resultResponse = validation.validateFields(bundles);
        if (!resultResponse.getResult()) {
            List<String> validations = (List<String>) resultResponse.getObject();

            logger.info("Obteniendo Cadena para Validar: " + " " + bundles.getCadena());

            model.addAttribute("fill_select", name_flows);
            model.addAttribute("fill_select2", utils.FilterSelect());
            model.addAttribute("messageValidation", "Has errors");
            model.addAttribute("validations", validations);
            model.addAttribute("bundles", bundles);
            return "flow/flow-dependencies";
        }

        if (bundleService.existsByNombre(bundles.getNombre())) {
            logger.info("Ya existe el flujo" + bundles.getNombre());
            model.addAttribute("fill_select", name_flows);
            model.addAttribute("fill_select2", utils.FilterSelect());
            model.addAttribute("message", "Error!" + " " + bundles.getNombre() + " " + "Ya Existe.");
            model.addAttribute("bundles", bundles);
            return "flow/flow-dependencies";
        }

        if (bundles.getCadena() == null || bundles.getCadena().equals("")) {
            model.addAttribute("fill_select", name_flows);
            model.addAttribute("fill_select2", utils.FilterSelect());
            model.addAttribute("message", "Error!" + " " + "maya dependencias no puede ser vacío.");
            model.addAttribute("bundles", bundles);
            return "flow/flow-dependencies";
        }

        String valida = utils.valida_dependencias(bundles.getCadena());
        if (valida != null) {
            model.addAttribute("fill_select", name_flows);
            model.addAttribute("fill_select2", utils.FilterSelect());
            model.addAttribute("message", valida);
            model.addAttribute("bundles", bundles);
            return "flow/flow-dependencies";
        }
        // utils.insert_dependencias(bundles.getCadena());

        //  Inician Métodos para Insertar
        logger.info("Insertando Dependencias ------------------");
        utils.insert_dependencias(bundles.getCadena());
        utils.create_wfxml(bundles.getNombre());

        try {
            logger.info("Creando Bundle ------------------");
            oozieClientConfig.CreateBundle(bundles.getNombre(), bundles.getFecha_inicio(), bundles.getFecha_fin(), bundles.getMinuto(), bundles.getHora(), bundles.getDiaSemana(), bundles.getMes(), bundles.getDiaMes());
        } catch (OozieClientException e) {
            logger.error(e.getMessage());
            model.addAttribute("message", e.getMessage());
            return "flow/flow-dependencies";

        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            model.addAttribute("message", e.getMessage());
            return "flow/flow-dependencies";
        }
        // return "redirect:/dependencies/create";
        return "redirect:/monitor/dependencies";
    }

    //Inicia Controlador para hacer Edicición de Las Dependencias.
    @GetMapping("/edit")
    public String edit(@RequestParam("nombre") String nombre, Model model) {

        logger.info(nombre);

        Wf_bundles bundles = bundleService.findByNombre(nombre);
        model.addAttribute("fill_select", utils.FilterSelect());
        model.addAttribute("getDependences", utils.getflows_dependences(nombre));
        model.addAttribute("bundles", bundles);
        model.addAttribute("nombre", nombre);
        return "flow/edit-dependencies";
    }

    @PostMapping("/saveChanges")
    public String save_changes(Wf_bundles bundles, Model model) {

        //Inicia Sección de Validaciones...
        ResultResponse resultResponse = validation.validateFields(bundles);
        if (!resultResponse.getResult()) {
            List<String> validations = (List<String>) resultResponse.getObject();
            model.addAttribute("fill_select", utils.FilterSelect());
            model.addAttribute("messageValidation", "Has errors");
            model.addAttribute("validations", validations);
            model.addAttribute("bundles", bundles);
            return "flow/edit-dependencies";
        }//Finaliza Sección de Validaciones.


        model.addAttribute("getDependences", utils.getflows_dependences(bundles.getNombre()));
        model.addAttribute("fill_select", utils.FilterSelect());
        model.addAttribute("bundles", bundles);

        String valida = utils.valida_dependencias(bundles.getCadena());
        if (valida != null) {
            model.addAttribute("getDependences", utils.getflows_dependences(bundles.getNombre()));
            model.addAttribute("fill_select", utils.FilterSelect());
            model.addAttribute("bundles", bundles);
            model.addAttribute("message", valida);
            return "flow/edit-dependencies";
        }

        //Eliminando Flujo de Oozie y de la tabala wf_bundles;
        try {
            oozieClientConfig.kill(bundles.getBundle_id());
        } catch (OozieClientException e) {
            e.printStackTrace();
        }
        bundleService.deleteByNombre(bundles.getNombre());//Eliminado Flujo de la tabala Wf_bundles.
        utils.delete_dependencies(bundles.getNombre());//Eliminando dependencias de la tabla Wf_flujos_dependencias.


        //Inician Métodos para Insertar
        logger.info("Insertando Dependencias ------------------");
        utils.insert_dependencias(bundles.getCadena());
        utils.create_wfxml(bundles.getNombre().trim());

        try {
            logger.info("Creando Bundle ------------------");
            oozieClientConfig.CreateBundle(bundles.getNombre(), bundles.getFecha_inicio(), bundles.getFecha_fin(), bundles.getMinuto(), bundles.getHora(), bundles.getDiaSemana(), bundles.getMes(), bundles.getDiaMes());
        } catch (OozieClientException e) {
            logger.error(e.getMessage());
            model.addAttribute("message", e.getMessage());
            return "redirect:/dependencies/edit";
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            model.addAttribute("message", e.getMessage());
            return "redirect:/dependencies/edit";
        }
        //return "monitor/dependencies";
        return "redirect:/monitor/dependencies";
    }
    //Finaliza  Controlador para hacer Edicición de Las Dependencias.

    @RequestMapping("/delete")
    public String delete(@RequestParam("nombre") String nombre, @RequestParam("bundle_id") String bundle_Id) {
        try {
            oozieClientConfig.kill(bundle_Id);
        } catch (OozieClientException e) {
            return "redirect:/dependencies/list";
        }
        bundleService.deleteByNombre(nombre); //Borra registros de la Tabla Bundles.
        utils.delete_dependencies(nombre);  //Borra las dependencias de la tabla Wf_flujos_dependencias.
        return "redirect:/dependencies/list";
    }

    @GetMapping("/reRun")
    public String RunBundle(String nombre, Model model) {
        logger.info(nombre);

        String mensaje = oozieClientConfig.RunBundle(nombre);
        if (mensaje != null) {
            model.addAttribute("dependenciesList", bundleService.findAll());
            model.addAttribute("messageValidation", mensaje);
            return "flow/list-flow-dependencies";
        } else {

            model.addAttribute("dependenciesList", bundleService.findAll());
            model.addAttribute("message", nombre + " ha sido ejecutado con éxito.");
            return "flow/list-flow-dependencies";
        }
    }
}