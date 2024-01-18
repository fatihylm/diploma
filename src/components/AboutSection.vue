<template>
    <div class="informations">
        <table class="Infos">
            <thead>
                <tr>
                    <th>Use Case</th>
                    <th>About Us</th>
                    <th>Green H2</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>
                        <img :src="ziel" alt="ziel" class="ziel"/> <br><br>
                        <p>The H2 Container delivers Data in form of CSV Files but these need to be converted first
                            This website is the sollution we came up with so Users can easily convert the Files and
                            use them in InfluxDB</p>
                    </td>
                    <td>
                        <img :src="team" alt="team" class="team"/> <br><br>
                        <p>This Website was made by two Students from the HTL Dornbirn It was a part of their Diploma
                            thesis.
                        </p>
                    </td>
                    <td>
                        <img :src="H2" alt="H2" class="h2"/> <br><br>
                        <p>Green H2 researched electrolysis with the innovative <br>use of conductive fibers as electrode
                            material. <br>The carbon fibers used have a large surface area and are cheaper than conventional
                            materials. This means that hydrogen can be used affordably to store electricity from renewable
                            energies</p>
                    </td>
                </tr>
            </tbody>
        </table>
    </div>
</template>
  
<script>
import team from '/src/images/team.jpg'
import H2 from '/src/images/H2.png'
import ziel from '/src/images/ziel.png'

export default {
  data() {
    return {
      team:team,
      H2:H2,
      ziel:ziel

    };
  }
};
</script>
  
<style>@import '/src/css/AboutSection.css';</style>
