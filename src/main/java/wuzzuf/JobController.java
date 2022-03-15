package wuzzuf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import java.util.stream.Collectors;

@Controller
@RequestMapping(path = "api/wuzzuf_jobs")
public class JobController {
    @Autowired
    private JobService jobService;


    public String buildTable(Dataset<?> dataset, String style) {
        String response = "<div style='height: 600px; overflow-y: auto; left: -30px;'><table " + style + "><tr><th>" + String.join("</th><th>", dataset.columns()) + "</th></tr>";

        response += dataset.toDF().collectAsList().stream().map(row -> row.mkString("<tr><td>", "</td><td>", "</td></tr>")).collect(Collectors.joining(""));

        response += "</table></div>";
        return response;
    }

    public String addChart(String table, String path) {
        String response = "<div style='float: left; width: 30%; padding: 0 20px 0 30px;'>" + table + "</div>";
        response += "<div style='float:right; width: 60%; padding: 50px 30px 0 20px;'> <img src='" + path + "' class='chart'> </div>";
        return response;
    }


    @GetMapping("/")
    public String home() {
        return "home";
    }

    @GetMapping(path = "sample")
    public String showSample(Model model){
        model.addAttribute("title", "Sample from Wuzzuf Jobs Dataset");
        model.addAttribute("response", buildTable(jobService.getSample(),"style='width: 90%;'"));
        return "index";
    }

    @GetMapping(path = "schema")
    public String showSchema(Model model){
        model.addAttribute("title", "Dataset Structure and Summary");
        model.addAttribute("response", jobService.getSchema());
        return "index";
    }

    @GetMapping(path = "clean")
    public String cleanData(Model model){
        model.addAttribute("title", "Cleaning Dataset");
        model.addAttribute("response", jobService.cleanData());
        return "index";
    }

    @GetMapping(path = "most_demanding_companies")
    public String jobsPerCompany(Model model){
        Dataset<Row> jobsPerCompany = jobService.jobsPerCompany();
        String table = buildTable(jobsPerCompany,"style='width: 100%;'");

//        jobService.createPieChart(jobsPerCompany, "Most Demanding Companies for Jobs Pie Chart", "companiesPieChart.png");
        String response = addChart(table, "/img/companiesPieChart.png");

        model.addAttribute("title", "Most Demanding Companies for Jobs");
        model.addAttribute("response", response);
        return "index";
    }

    @GetMapping(path = "most_popular_job_titles")
    public String mostPopularJobTitles(Model model){
        Dataset<Row> mostPopularJobs = jobService.mostPopularJobTitles();
        String table = buildTable(mostPopularJobs,"style='width: 100%'");

//        jobService.createBarChart(mostPopularJobs, "Most Popular Job Titles Bar Chart", "Job Titles", "Number of vacancies", "jobsBarChart.png");
        String response = addChart(table, "/img/jobsBarChart.png");

        model.addAttribute("title", "Most Popular Job Titles");
        model.addAttribute("response", response);
        return "index";
    }

    @GetMapping(path = "most_popular_areas")
    public String mostPopularAreas(Model model){
        Dataset<Row> mostPopularAreas = jobService.mostPopularAreas();
        String table = buildTable(mostPopularAreas,"style='width: 100%'");

//        jobService.createBarChart(mostPopularAreas, "Most Popular Areas Bar Chart", "Location", "Number of vacancies", "areasBarChart.png");
        String response = addChart(table, "/img/areasBarChart.png");

        model.addAttribute("title", "Most Popular Areas");
        model.addAttribute("response", response);
        return "index";
    }

    @GetMapping(path = "most_required_skills")
    public String mostRequiredSkills(Model model){
        model.addAttribute("title", "Most Important Skills Required");
        model.addAttribute("response", buildTable(jobService.mostRequiredSkills(),"style='width: 50%;'"));
        return "index";
    }
}
